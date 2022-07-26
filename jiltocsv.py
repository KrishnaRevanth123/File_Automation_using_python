import os
import re
import csv

from datetime import datetime
from fileinput import filename


def JILtoCSV(filename):
    print("Jil to CSV")
    jilFileName = filename
    output_file = os.path.splitext(jilFileName)[0] + ".csv"
    count = 0
    jilinArray = []
    print("Jobs Extracted")
    oneJob = {}
    with open(jilFileName, "rt") as jil:
        jilLines = jil.readlines()
        for linesInJill in jilLines:
            if "insert_job:" in linesInJill:
                jilinArray.append(oneJob)
                linesInJill = linesInJill.strip()
                jobName = re.findall(r'insert_job:(.*?)job_type:', linesInJill)[0]
                jobType = linesInJill.split("job_type:")[1]
                oneJob = {}
                oneJob["jobName"] = str(jobName).strip()
                oneJob["jobType"] = str(jobType).strip()
                count += 1
                print(f"{count}", end="\r")
            else:
                linesInJill = linesInJill.strip()
                if linesInJill != "\n" and "/* ----" not in linesInJill and linesInJill != "":
                    if "start_times" in linesInJill:
                        spli = linesInJill.split("start_times:")
                        oneJob["start_times"] = str(spli[1]).replace("\"", "")
                    elif "command:" in linesInJill:
                        spli = linesInJill.split("command:")
                        oneJob["command"] = str(spli[1]).strip()
                    else:
                        spli = linesInJill.split(":", 1)
                        oneJob[str(spli[0]).strip()] = str(spli[1]).strip().replace("\"", "")
        jilinArray.append(oneJob)
        print(count)

    fieldnames_1 = ["jobName", "jobType", "box_name", "command", "machine", "owner", "permission", "date_conditions",
                    "days_of_week", "start_times", "start_mins", "run_window", "run_calendar", "exclude_calendar",
                    "condition", "alarm_if_fail", "max_run_alarm", "min_run_alarm", "must_start_times", "description",
                    "std_out_file", "std_err_file", "watch_file", "watch_interval", "watch_file_min_size",
                    "box_success", "term_run_time", "max_exit_success", "box_terminator", "job_terminator", "group",
                    "application", "send_notification", "profile", "job_load", "n_retrys", "envvars", "timezone",
                    "elevated", "resources", "priority", "notification_emailaddress", "notification_msg", "std_in_file",
                    "fail_codes", "box_failure", "interactive", "job_class", "success_codes", "auto_hold", "ulimit",
                    "ftp_local_name", "ftp_local_name_1", "ftp_remote_name", "ftp_server_name", "ftp_server_port",
                    "ftp_transfer_direction", "ftp_transfer_type", "ftp_use_ssl", "ftp_user_type", "sap_chain_id",
                    "sap_client", "sap_job_count", "sap_job_name", "sap_lang", "sap_mon_child", "sap_office",
                    "sap_release_option", "sap_rfc_dest", "sap_step_parms", "scp_local_name", "scp_local_user",
                    "scp_protocol", "scp_remote_dir", "scp_remote_name", "scp_server_name", "scp_server_name_2",
                    "scp_server_port", "scp_target_os", "scp_transfer_direction"]
    fieldnames_2 = set()
    for i in jilinArray:
        fieldnames_2.update(list(i.keys()))
    fieldnames_2 = list(fieldnames_2)
    fieldnames = []
    for field in fieldnames_1:
        if field in fieldnames_2:
            fieldnames.append(field)
    for field in fieldnames_2:
        if field not in fieldnames:
            fieldnames.append(field)

    print("Inserting Jobs to CSV selected Fields are \n")
    print(fieldnames)
    print("Number Of jobs Processed")
    count = 0
    jilinArray.pop(0)
    rows = []
    for ar in jilinArray:
        print(f"{count}", end="\r")
        values = []
        for k in fieldnames:
            if k in ar:
                values.append(ar[k])
            else:
                values.append(None)
        rows.append(values)
        count += 1
    print(count)
    with open(output_file, 'w', newline="") as fi:
        write = csv.writer(fi)
        write.writerow(fieldnames)
        write.writerows(rows)
    print(f"CSV file created at {output_file}")
    # response = User(name=filename, status="converted to csv Successfully", time=datetime.now(), state="csvFile")
    # response.save()
    return output_file

