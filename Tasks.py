import paramiko
import Creds
import os
import dbMgr
import subprocess

class tasks:

    def __init__(self):
        self.user_name = Creds.user_name
        self.password = Creds.password
        self.host_name = Creds.host_name
        self.ssh = paramiko.SSHClient()
        self.db = dbMgr.OcelliDB()
        self.proceed = False

    def connect(self):

        try:

            self.ssh.load_system_host_keys()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(self.host_name, username=self.user_name, password=self.password)

        except Exception as e:
            print(e)

    def disconnect(self):
        try:
            self.ssh.close()
        except Exception as e:
            print e

    def get_disk_space(self):
        self.connect()
        disk_list = []

        # Check the disk space
        (stdin, stdout, stderr) =self.ssh.exec_command("cd ../../; df -h;")

        # Push all the disks to a list
        for line in stdout.readlines():
            disk_list.append(line)

        self.disconnect()

        # check to see if any of the disks are getting close to full
        for disk in disk_list:
            print disk

    def check_running_processes(self):
        self.connect()

        (stdin, stdout, stderr) = self.ssh.exec_command("ps -ef | grep python")

        for line in stdout.readlines():
            print line

        self.disconnect()

    def check_table_size(self):
        db = dbMgr.DbMgr()

        query = """SELECT nspname || '.' || relname AS relation,
                    pg_size_pretty(pg_total_relation_size(C.oid)) AS total_size
                  FROM pg_class C
                  LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
                  WHERE nspname NOT IN ('pg_catalog', 'information_schema')
                    AND C.relkind <> 'i'
                    AND nspname !~ '^pg_toast'
                  ORDER BY pg_total_relation_size(C.oid) DESC;"""

        results = db.query(query)
        for result in results:
            size = result["total_size"].split(" ")
            if size[0] != 0:
                if size[1] == "MB":
                    size = int(size[0])
                    if size > 2500:
                        print result

    # need to add in functionality to remove the old log themseleves
    def del_apache_log(self):

        self.connect()
        logs_list = []
        (stdin, stdout, stderr) = self.ssh.exec_command("cd ../../; cd /etc/httpd/logs/;")

        for line in stdout.readlines():
            logs_list.append(line)

        self.disconnect()

    def get_server_locs(self):
        query = "select distinct(loc_server), loc_id, loc_pwd from e1n_logparse_location where client_id = {0} and loc_active = 'Y';".format(self.client_id)
        results = self.db.query2(query)

        return results

    def get_log_locs(self, server_ip):
        query = "select loc_key, loc_server, loc_dir, loc_share, loc_begin, loc_ext, loc_type_id " \
                "from e1n_logparse_location where client_id = {1} and loc_server = '{0}'".format(server_ip, self.client_id)

        results = self.db.query2(query)

        return results

    def dynamic_login(self, ip, user_name, password):
        try:
            self.ssh.load_system_host_keys()
            self.ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.ssh.connect(ip, username=user_name, password=password)
            self.proceed = True
        except Exception as e:
            self.proceed = False

    def netuse_connect(self, server, dir_path):
        logs_found = ''
        conn_path = '\\\\' + server["loc_server"] + '\\' + dir_path
        conn_path = conn_path.replace(':', '$', 1)

        windows_connection = 'NET USE ' + conn_path + ' /User:' + server['loc_id'] + \
                             ' ' + server['loc_pwd']

        process = subprocess.Popen(windows_connection, shell=False, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)
        out, err = process.communicate()

        errcode = process.returncode

        if errcode != 0:
            return logs_found


        else:

            logs_found = os.listdir(conn_path)

            # Close the connection
            close_connection = 'NET USE /delete /y ' + conn_path

            process = subprocess.Popen(close_connection, stdout=subprocess.PIPE, shell=False)

            out, err = process.communicate()
            return logs_found

    def get_clients(self):

        query = "select DISTINCT client_id "" \
        ""from e1n_eum_client_install "" \
        where ini_file_path is not null and loc_key is null and ini_file is not null and client_id > 2 order by client_id;"

        results = self.db.query2(query)

        return results


    def check_eum_servers(self):
        try:
            clients = self.get_clients()
            for client in clients:
                self.client_id = client["client_id"]
                logs_dir = {}
                server_list = []
                server_dict = self.get_server_locs()
                for server in server_dict:

                    db_locations = self.get_log_locs(server["loc_server"])

                    for loc in db_locations:
                        final_dict = {}
                        loc_share = loc["loc_share"]
                        loc_key = loc["loc_key"]
                        loc_begin = loc["loc_begin"]
                        loc_dir = loc["loc_dir"]
                        loc_os = loc["loc_type_id"]
                        dir_path = loc_share + loc_dir

                        # Break everything down by type

                        if loc_os is 1:
                            # ISeries
                            print("ISERIES")
                            t = 1
                        elif loc_os is 2:
                            # Linux
                            self.dynamic_login(server["loc_server"], server["loc_id"], server["loc_pwd"])

                            if self.proceed:
                                stdin, stdout, stderr = self.ssh.exec_command("cd /.; cd " + dir_path + ";ls;")

                                # Read all of the files inside of the directory
                                for line in stdout.readlines():
                                    # Check if the log starting location is in the directory
                                    if loc_begin in line:
                                        # Create another list with logparse loc_key, full file path and the hostname of the server
                                        final_dict["loc_key"] = loc_key
                                        final_dict["dir_path"] = dir_path

                                        stdin, stdout, stderr = self.ssh.exec_command("cd /. ; hostname")
                                        for line in stdout.readlines():
                                            final_dict["instance_name"] = line
                                        server_list.append(final_dict)
                                        break

                                logs_dir[server["loc_server"]] = server_list
                            if self.proceed:
                                for result in logs_dir:
                                    for loc in logs_dir[result]:
                                        loc_key = loc["loc_key"]
                                        host_name = (loc["instance_name"])
                                        results = self.write_query(host_name)
                                        if results != []:
                                            for ini_file_info in results:
                                                ini_path = ini_file_info["ini_file_path"]
                                                ini_file = ini_file_info["ini_file"]

                                                stdin, stdout, stderr = self.ssh.exec_command("cd /.; cd " + ini_path + ";ls;")
                                                ini_file = ini_file.split(',')
                                                # Read all of the files inside of the directory
                                                for line in stdout.readlines():
                                                    if ini_file[0] in line:
                                                        # Write update Query here after verifying the ini directory
                                                        self.update_query(ini_file_info["install_key"], loc_key)

                        elif loc_os is 3:
                            try:
                                results = None
                                # Windows

                                final_dict["loc_key"] = loc_key
                                final_dict["dir_path"] = dir_path

                                # Get the host name of the server
                                conn_path = r"\\" + server["loc_server"] + " -u " + server["loc_id"] + " -p " + server[
                                    "loc_pwd"]

                                # command = ("C:\Users\shonsaker\Documents\Psexec.exe " + conn_path + " hostname")
                                command = ("C:\Users\shonsaker\Documents\Psexec.exe " + conn_path + " ping -a " + server["loc_server"])

                                process = subprocess.Popen(command,
                                                           stdout=subprocess.PIPE,
                                                           shell=True)

                                hostname, err = process.communicate()

                                hostname = hostname.split("Pinging ")
                                host_name = hostname[1].split(r" [")

                                final_dict["instance_name"] = host_name[0]
                                final_dict["server_ip"] = server["loc_server"]

                                server_list.append(final_dict)

                                # for result in logs_dir:
                                for loc in server_list:
                                    loc_key = loc["loc_key"]
                                    host_name = (loc["instance_name"])
                                    results = self.write_windows_query(host_name)
                                    if results != []:
                                        for ini_file_info in results:
                                            ini_path = ini_file_info["ini_file_path"]
                                            ini_file = ini_file_info["ini_file"]
                                            instance_name = ini_file_info["instance_name"]

                                            # Check to see if the ini file exists inside the server

                                            conn_path = '\\\\' + server["loc_server"] + '\\' + ini_path
                                            conn_path = conn_path.replace(':', '$', 1)

                                            windows_connection = 'NET USE ' + conn_path + ' /User:' + server['loc_id'] + \
                                                                 ' ' + server['loc_pwd']

                                            process = subprocess.Popen(windows_connection, shell=False, stdout=subprocess.PIPE,
                                                                       stderr=subprocess.PIPE)
                                            out, err = process.communicate()

                                            errcode = process.returncode

                                            if errcode != 0:
                                                pass

                                            else:

                                                logs_found = os.listdir(conn_path)

                                                # Close the connection
                                                close_connection = 'NET USE /delete /y ' + conn_path

                                                process = subprocess.Popen(close_connection, stdout=subprocess.PIPE, shell=False)

                                                out, err = process.communicate()

                                            ini_file = ini_file.split(',')
                                            ini_file = ini_file[0].lower()
                                            # Read all of the files inside of the directory
                                            for line in logs_found:
                                                if ini_file[0] in line:
                                                    # Write update Query here after verifying the ini directory
                                                    self.update_windows_query(ini_file_info["install_key"], loc_key, instance_name)
                                                    break
                            except Exception as e:
                                pass

        except Exception as e:
            print(e)

        finally:
            self.disconnect()

    def update_query(self, install_key, loc_key):
        try:
            query = """update e1n_eum_client_install set loc_key = {0}
                        where
                        client_id = {2} and install_key = ({1}) and (
                        server_type = 'webserver' or server_type = 'entserver')""".format(loc_key, install_key, self.client_id)
            print query
            results = self.db.ins_upd2(query)
            if results ==0:
                print "Linux Successfully Updated"

        except Exception as e:
            print(e)

    def write_query(self, host_name):
        try:
            host_name = host_name.strip("\n")

            query = """select *
                        from e1n_eum_client_install
                        where
                        client_id = {1} and host_name = lower('{0}') and (
                        server_type = 'webserver' or server_type = 'entserver')
                        order
                        by
                        install_key;""".format(host_name, self.client_id)

            results = self.db.query2(query)
            return results
        except Exception as e:
            print(e)


    def write_windows_query(self, host_name):
        # host_name = host_name.strip("\n")

        query = """select *
                    from e1n_eum_client_install
                    where
                    client_id = {1} and host_name = '{0}' and (
                    server_type = 'webserver' or server_type = 'entserver')
                    order
                    by
                    install_key;""".format(host_name, self.client_id)
        results = self.db.query2(query)
        return results


    def update_windows_query(self, install_key, loc_key, host_name):
        try:
            query = """update e1n_eum_client_install set loc_key = {0}
                        where
                        client_id = {2} and install_key = ({1}) and instance_name = '{3}' and (
                        server_type = 'webserver' or server_type = 'entserver')""".format(loc_key, install_key, self.client_id, host_name)
            print query
            results = self.db.ins_upd2(query)
            # results = []
            if results == 0:
                print("updated!")
        except Exception as e:
            print(e)


















