/*
 * monitor.c - Multi-Container Memory Monitor (Linux Kernel Module)
 *
 * Provided boilerplate:
 *   - device registration and teardown
 *   - timer setup
 *   - RSS helper
 *   - soft-limit and hard-limit event helpers
 *   - ioctl dispatch shell
 *
 * YOUR WORK: Fill in all sections marked // TODO.
 */

#include <linux/cdev.h>
#include <linux/device.h>
#include <linux/fs.h>
#include <linux/kernel.h>
#include <linux/list.h>
#include <linux/mm.h>
#include <linux/module.h>
#include <linux/mutex.h>
#include <linux/pid.h>
#include <linux/sched/signal.h>
#include <linux/slab.h>
#include <linux/spinlock.h>
#include <linux/string.h>
#include <linux/timer.h>
#include <linux/uaccess.h>
#include <linux/version.h>

#include "monitor_ioctl.h"

#define DEVICE_NAME "container_monitor"
#define CHECK_INTERVAL_SEC 1

struct monitored_entry {
    pid_t pid;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    bool soft_limit_emitted;
    char container_id[MONITOR_NAME_LEN];
    struct list_head list;
};

static LIST_HEAD(monitored_list);
static spinlock_t monitored_lock;


/* --- Provided: internal device / timer state --- */
static struct timer_list monitor_timer;
static dev_t dev_num;
static struct cdev c_dev;
static struct class *cl;

static struct monitored_entry *find_entry_locked(pid_t pid, const char *container_id)
{
    struct monitored_entry *entry;

    list_for_each_entry(entry, &monitored_list, list) {
        if (entry->pid == pid &&
            strncmp(entry->container_id, container_id, sizeof(entry->container_id)) == 0)
            return entry;
    }

    return NULL;
}

static int entry_after_cursor(const struct monitored_entry *entry,
                              bool have_cursor,
                              pid_t last_pid,
                              const char *last_id)
{
    int cmp;

    if (!have_cursor)
        return 1;

    if (entry->pid > last_pid)
        return 1;
    if (entry->pid < last_pid)
        return 0;

    cmp = strncmp(entry->container_id, last_id, sizeof(entry->container_id));
    return cmp > 0;
}

/* ---------------------------------------------------------------
 * Provided: RSS Helper
 *
 * Returns the Resident Set Size in bytes for the given PID,
 * or -1 if the task no longer exists.
 * --------------------------------------------------------------- */
static long get_rss_bytes(pid_t pid)
{
    struct task_struct *task;
    struct mm_struct *mm;
    long rss_pages = 0;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (!task) {
        rcu_read_unlock();
        return -1;
    }
    get_task_struct(task);
    rcu_read_unlock();

    mm = get_task_mm(task);
    if (mm) {
        rss_pages = get_mm_rss(mm);
        mmput(mm);
    }
    put_task_struct(task);

    return rss_pages * PAGE_SIZE;
}

/* ---------------------------------------------------------------
 * Provided: soft-limit helper
 *
 * Log a warning when a process exceeds the soft limit.
 * --------------------------------------------------------------- */
static void log_soft_limit_event(const char *container_id,
                                 pid_t pid,
                                 unsigned long limit_bytes,
                                 long rss_bytes)
{
    printk(KERN_WARNING
           "[container_monitor] SOFT LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Provided: hard-limit helper
 *
 * Kill a process when it exceeds the hard limit.
 * --------------------------------------------------------------- */
static void kill_process(const char *container_id,
                         pid_t pid,
                         unsigned long limit_bytes,
                         long rss_bytes)
{
    struct task_struct *task;

    rcu_read_lock();
    task = pid_task(find_vpid(pid), PIDTYPE_PID);
    if (task)
        send_sig(SIGKILL, task, 1);
    rcu_read_unlock();

    printk(KERN_WARNING
           "[container_monitor] HARD LIMIT container=%s pid=%d rss=%ld limit=%lu\n",
           container_id, pid, rss_bytes, limit_bytes);
}

/* ---------------------------------------------------------------
 * Timer Callback - fires every CHECK_INTERVAL_SEC seconds.
 * --------------------------------------------------------------- */
static void timer_callback(struct timer_list *t)
{
    bool have_cursor = false;
    pid_t last_pid = 0;
    char last_id[MONITOR_NAME_LEN] = {0};

    (void)t;

    for (;;) {
        struct monitored_entry *candidate = NULL;
        struct monitored_entry *entry;
        unsigned long flags;
        pid_t pid;
        unsigned long soft_limit;
        unsigned long hard_limit;
        char container_id[MONITOR_NAME_LEN];
        long rss_bytes;
        bool emit_soft = false;
        bool hard_kill = false;
        bool remove_entry = false;

        spin_lock_irqsave(&monitored_lock, flags);
        list_for_each_entry(entry, &monitored_list, list) {
            if (entry_after_cursor(entry, have_cursor, last_pid, last_id)) {
                candidate = entry;
                break;
            }
        }

        if (!candidate) {
            spin_unlock_irqrestore(&monitored_lock, flags);
            break;
        }

        pid = candidate->pid;
        soft_limit = candidate->soft_limit_bytes;
        hard_limit = candidate->hard_limit_bytes;
        strscpy(container_id, candidate->container_id, sizeof(container_id));
        spin_unlock_irqrestore(&monitored_lock, flags);

        rss_bytes = get_rss_bytes(pid);

        spin_lock_irqsave(&monitored_lock, flags);
        entry = find_entry_locked(pid, container_id);
        if (entry) {
            if (rss_bytes < 0) {
                list_del(&entry->list);
                remove_entry = true;
            } else if ((unsigned long)rss_bytes > entry->hard_limit_bytes) {
                list_del(&entry->list);
                remove_entry = true;
                hard_kill = true;
            } else if (!entry->soft_limit_emitted &&
                       (unsigned long)rss_bytes > entry->soft_limit_bytes) {
                entry->soft_limit_emitted = true;
                emit_soft = true;
            }
        }
        spin_unlock_irqrestore(&monitored_lock, flags);

        if (emit_soft)
            log_soft_limit_event(container_id, pid, soft_limit, rss_bytes);
        if (hard_kill)
            kill_process(container_id, pid, hard_limit, rss_bytes);
        if (remove_entry && entry)
            kfree(entry);

        have_cursor = true;
        last_pid = pid;
        strscpy(last_id, container_id, sizeof(last_id));
    }

    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);
}

/* ---------------------------------------------------------------
 * IOCTL Handler
 *
 * Supported operations:
 *   - register a PID with soft + hard limits
 *   - unregister a PID when the runtime no longer needs tracking
 * --------------------------------------------------------------- */
static long monitor_ioctl(struct file *f, unsigned int cmd, unsigned long arg)
{
    struct monitor_request req;

    (void)f;

    if (cmd != MONITOR_REGISTER && cmd != MONITOR_UNREGISTER)
        return -EINVAL;

    if (copy_from_user(&req, (struct monitor_request __user *)arg, sizeof(req)))
        return -EFAULT;

    if (cmd == MONITOR_REGISTER) {
        struct monitored_entry *entry;
        unsigned long flags;

        printk(KERN_INFO
               "[container_monitor] Registering container=%s pid=%d soft=%lu hard=%lu\n",
               req.container_id, req.pid, req.soft_limit_bytes, req.hard_limit_bytes);

        if (req.pid <= 0 || req.soft_limit_bytes > req.hard_limit_bytes)
            return -EINVAL;

        entry = kzalloc(sizeof(*entry), GFP_KERNEL);
        if (!entry)
            return -ENOMEM;

        entry->pid = req.pid;
        entry->soft_limit_bytes = req.soft_limit_bytes;
        entry->hard_limit_bytes = req.hard_limit_bytes;
        entry->soft_limit_emitted = false;
        strscpy(entry->container_id, req.container_id, sizeof(entry->container_id));

        spin_lock_irqsave(&monitored_lock, flags);
        if (find_entry_locked(req.pid, req.container_id)) {
            spin_unlock_irqrestore(&monitored_lock, flags);
            kfree(entry);
            return -EEXIST;
        }
        list_add_tail(&entry->list, &monitored_list);
        spin_unlock_irqrestore(&monitored_lock, flags);

        return 0;
    }

    printk(KERN_INFO
           "[container_monitor] Unregister request container=%s pid=%d\n",
           req.container_id, req.pid);

    {
        struct monitored_entry *entry;
        unsigned long flags;

        spin_lock_irqsave(&monitored_lock, flags);
        entry = find_entry_locked(req.pid, req.container_id);
        if (!entry) {
            spin_unlock_irqrestore(&monitored_lock, flags);
            return -ENOENT;
        }
        list_del(&entry->list);
        spin_unlock_irqrestore(&monitored_lock, flags);
        kfree(entry);
    }

    return 0;
}

/* --- Provided: file operations --- */
static struct file_operations fops = {
    .owner = THIS_MODULE,
    .unlocked_ioctl = monitor_ioctl,
};

/* --- Provided: Module Init --- */
static int __init monitor_init(void)
{
    spin_lock_init(&monitored_lock);

    if (alloc_chrdev_region(&dev_num, 0, 1, DEVICE_NAME) < 0)
        return -1;

#if LINUX_VERSION_CODE >= KERNEL_VERSION(6, 4, 0)
    cl = class_create(DEVICE_NAME);
#else
    cl = class_create(THIS_MODULE, DEVICE_NAME);
#endif
    if (IS_ERR(cl)) {
        unregister_chrdev_region(dev_num, 1);
        return PTR_ERR(cl);
    }

    if (IS_ERR(device_create(cl, NULL, dev_num, NULL, DEVICE_NAME))) {
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    cdev_init(&c_dev, &fops);
    if (cdev_add(&c_dev, dev_num, 1) < 0) {
        device_destroy(cl, dev_num);
        class_destroy(cl);
        unregister_chrdev_region(dev_num, 1);
        return -1;
    }

    timer_setup(&monitor_timer, timer_callback, 0);
    mod_timer(&monitor_timer, jiffies + CHECK_INTERVAL_SEC * HZ);

    printk(KERN_INFO "[container_monitor] Module loaded. Device: /dev/%s\n", DEVICE_NAME);
    return 0;
}

/* --- Provided: Module Exit --- */
static void __exit monitor_exit(void)
{
    struct monitored_entry *entry, *tmp;
    unsigned long flags;

    timer_shutdown_sync(&monitor_timer);

    spin_lock_irqsave(&monitored_lock, flags);
    list_for_each_entry_safe(entry, tmp, &monitored_list, list) {
        list_del(&entry->list);
        kfree(entry);
    }
    spin_unlock_irqrestore(&monitored_lock, flags);

    cdev_del(&c_dev);
    device_destroy(cl, dev_num);
    class_destroy(cl);
    unregister_chrdev_region(dev_num, 1);

    printk(KERN_INFO "[container_monitor] Module unloaded.\n");
}

module_init(monitor_init);
module_exit(monitor_exit);

MODULE_LICENSE("GPL");
MODULE_DESCRIPTION("Supervised multi-container memory monitor");
