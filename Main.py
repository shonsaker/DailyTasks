import Tasks

def main():
    # Run all the code here
    task = Tasks.tasks()
    # task.get_disk_space()
    # task.check_running_processes()
    # task.check_table_size()
    task.check_eum_servers()

if __name__ == '__main__':
    main()