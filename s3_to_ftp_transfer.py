import ftplib
import os
import sys
import traceback
import logging
import datetime
import time
import io
import boto3
import ntpath
# from urllib.parse import urlparse
import threading
# from urlparse import urlparse
# s3= boto3.resource('s3')
from ftplib import FTP
STATUS_KEY = "status"
RESULT_KEY = "result"
FAILED_KEY = "FAILED"
SUCCESS_KEY = "SUCCESS"
ERROR_KEY = "error"
USER = ''
PASS = ''
SERVER = ''
PORT = ""
Location = ""
address = ''
write_path = ""
s3_target_path = ""
destination_path = ""
s3_path = ""
file_name = ""
ftp_path = ""
success_file_fri = ""
success_file = [""]
weekly_run = ["",""]
start_time = datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
threshold_time = datetime.datetime.now()
sleep_time = 100
# session = ftplib.FTP(SERVER, USER, PASS)
# print(session)


def connect_ftp():
    # Connect to the server
    try:
        status_message = "starting to establish connection to FTP server"
        logging.info(status_message)
        # session = ftplib.FTP(SERVER, USER, PASS)
        session = ftplib.FTP(SERVER)
        session.login(USER, PASS)
        session.cwd("<ftp loaction>")
        print(session.getwelcome())
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: session}
        status_message = "ending of establish connection to FTP server function"
        logging.info(status_message)
    except:
        error_message = status_message + ": " + str(traceback.format_exc())
        logging.error(str(error_message))
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


def list_files_in_ftp(session):
    try:
        status_message = "staring to list files in ftp location"
        files = session.nlst()
        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: files}
    except:
        raise Exception


def chack_success_file(file_list):
    try:
        status_message = "starting to get success file present or not"
        logging.info(status_message)
        for file in file_list:
            for run in weekly_run:
                for suc_file in success_file:
                    if run == "friday" and suc_file == file:
                        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: "Y"}
                    if run == "thursday" and suc_file == file:
                        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: "Y"}
                    else:
                        return {STATUS_KEY: SUCCESS_KEY, RESULT_KEY: "N"}
        status_message = "ending method to get success file status"
        logging.info(status_message)
    except:
        error_message = status_message + ": " + str(traceback.format_exc())
        logging.error(str(error_message))
        return {STATUS_KEY: FAILED_KEY, RESULT_KEY: "N", ERROR_KEY: error_message}


def trigger_ftp_to_s3_transfer(success_file_flag, session, ftp_file_list):
    try:
        status_message = "starting method to transfer files from ftp to s3"
        logging.info(status_message)
        if success_file_flag == "Y":
            # file_list = self.list_files_in_ftp(session)
            # file_list = file_list[RESULT_KEY]
            # for x in range(0, len(file_list) - 1):
            #     myfile = io.BytesIO()
            #     filename = 'RETR ' + file_list[x]
            #     resp = session.retrbinary(filename, myfile.write)
            #     myfile.seek(0)
            #     path = address + file_list[x]
            #     # putting file on s3
            #     s3.Object(s3Bucketname, path).put(Body=myfile)
            for files in ftp_file_list:
                print(files)
                # ftp_path = Location+"/"+files
                # print(ftp_path)
                # file = open(ftp_path, 'rb')
                # print file
                session.retrbinary("RETR " + files, open(os.path.join(write_path, files), 'wb').write)
                # self.upload_s3_object(os.path.join(write_path,files), s3_target_path)
                # session.storbinary('STOR ' + s3_path + files, file)
                # file.close()
            session.quit()
        if success_file_flag == "N":
            while True:
                current_time = datetime.datetime.strftime(datetime.datetime.utcnow(), "%Y-%m-%d %H:%M:%S")
                if datetime.datetime.strptime(current_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(
                    start_time, "%Y-%m-%d %H:%M:%S") <= threshold_time:
                # if current_time - start_time <= threshold_time:
                    time.sleep(sleep_time)
                file_list = list_files_in_ftp(session)
                ftp_file_list = file_list[RESULT_KEY]
                output_status = chack_success_file(ftp_file_list)
                success_file_flag = output_status[RESULT_KEY]
                if success_file_flag == "Y":
                    trigger_ftp_to_s3_transfer(success_file_flag, session, ftp_file_list)
                    break
        status_message = "ending method for transfer files from ftp to s3"
        logging.info(status_message)
        return {STATUS_KEY: SUCCESS_KEY}
    except:
        error_message = status_message + ": " + str(traceback.format_exc())
        logging.error(str(error_message))
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


def upload_s3_object(local_path, s3_path):
    """
        Purpose     :   uploading a file from ec2 machine to s3
                            a. if path of directory(ie. /dir1) mentioned,lists all the files in the directory and uploads to s3
                            b. if path inside the directory (ie. /dir1/ )mentioned ,lists all the files and uploads to s3
                            c.if patn of file mentioned,uploads it to s3
        Input       :   local path(ie. /<filepath>),s3 path(i.e s3://<bucket-name>/<filepath>  )
        Output      :   Return status SUCCESS/FAILED , result SUCCESS/FAILED , error ERROR TRACEBACK
    """
    status_message = ""
    try:
        status_message = "starting upload_s3_object object"
        logging.info(status_message)
        output = urlparse(s3_path)
        bucket_name = output.netloc
        key_path = output.path.lstrip("/")
        logging.debug("local path - " + str(local_path))
        logging.debug("s3_path - " + str(s3_path))
        if not (os.path.exists(local_path)):
            return {STATUS_KEY: FAILED_KEY, ERROR_KEY: "source path does not exist"}

        if os.path.isdir(local_path):
            for file_name in os.listdir(local_path):
                src_file_path = os.path.join(local_path, file_name)
                target_file_path = os.path.join(key_path, file_name)

                client.upload_file(src_file_path, bucket_name, target_file_path)

                status_message = "uploading file " + src_file_path + " to s3://" + target_file_path
                logging.debug(status_message)

            status_message = "completing upload_s3_object object"
            logging.debug(status_message)

        elif os.path.isfile(local_path):

            logging.debug("It is a normal file")
            src_file_path = local_path
            logging.debug("file name" + ntpath.basename(local_path))
            target_file_path = os.path.join(key_path, ntpath.basename(local_path))
            logging.debug("target file path " + target_file_path)
            client.upload_file(src_file_path, bucket_name, target_file_path)

        logging.info("Completing upload of s3 object")
        return {STATUS_KEY: SUCCESS_KEY}
    except:
        error_message = status_message + ": " + str(traceback.format_exc())
        logging.error(error_message)
        return {STATUS_KEY: FAILED_KEY, ERROR_KEY: error_message}


def main():
    output_status = connect_ftp()
    if output_status[STATUS_KEY] == FAILED_KEY:
        return output_status
    session = output_status[RESULT_KEY]
    print(session)

    output_status = list_files_in_ftp(session)
    if output_status[STATUS_KEY] == FAILED_KEY:
        return output_status
    ftp_file_list = output_status[RESULT_KEY]
    print(output_status)

    output_status = chack_success_file(ftp_file_list)
    print(output_status)
    if output_status[STATUS_KEY] == FAILED_KEY:
        return output_status
    success_file_flag = output_status[RESULT_KEY]
    print(success_file_flag)

    success_file_flag = "Y"

    n = 4
    final_list = [ftp_file_list[i * n:(i + 1) * n] for i in range((len(ftp_file_list) + n - 1) // n)]
    print(final_list)

    for file_list in final_list:
        t1 = threading.Thread(target=trigger_ftp_to_s3_transfer, args=(success_file_flag, session, file_list[0],))
        t2 = threading.Thread(target=trigger_ftp_to_s3_transfer, args=(success_file_flag, session, file_list[1],))
        t3 = threading.Thread(target=trigger_ftp_to_s3_transfer, args=(success_file_flag, session, file_list[2],))
        t4 = threading.Thread(target=trigger_ftp_to_s3_transfer, args=(success_file_flag, session, file_list[3],))
        # output_status = self.trigger_ftp_to_s3_transfer(success_file_flag,session,ftp_file_list)
        # if output_status[STATUS_KEY] == FAILED_KEY:
        #     return output_status
