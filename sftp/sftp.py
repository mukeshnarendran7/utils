# ------------------------------------------------------------------------------
# Written by Mukesh Narendran 
# Connects with sftp server, upload, download folders & files
# ------------------------------------------------------------------------------
import os
import stat
import logging
import datetime
import paramiko
from datetime import datetime
from stat import S_ISDIR, S_ISREG


def create_log():
        """Generate log file folder"""
        fold = os.path.join(os.getcwd(), "log")
        if not os.path.exists(fold):
                os.mkdir(fold)
        else:
                print("log folder present")
        
        # open logger
        nm = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
        nm = nm.replace(' ', '_').replace('/', '_')
        log_file = os.path.join(fold, nm+".log")    
        return log_file

# Create log file
log_fil = create_log()
logging.basicConfig(
                filename=log_fil,
                format='%(asctime)s:%(levelname)s:%(message)s',
                level=logging.INFO)


class Mysftp:
        def __init__(self, params):
                self.params = params
        
        def connect_sftp(self):
                '''Generate connection with SFTP server
                '''        
                try:
                        info = self.params["site"]
                        client = paramiko.SSHClient()
                        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                        client.connect(hostname=info["host"],
                                username=info["user"],
                                port=info["port"],
                                password=info["password"],
                                allow_agent=False,
                                look_for_keys=False
                                )
                        sft = client.open_sftp()

                        logging.info(f"Connected has been established with {info['host']}")
                except paramiko.AuthenticationException:
                        logging.exception("Authentication failed, please verify your credentials.")
                except paramiko.SSHException as sshException:
                        logging.exception(f"Could not establish SSH connection: {sshException}")
                except paramiko.BadHostKeyException as badHostKeyException:
                        logging.exception(f"Bad host key: {badHostKeyException}")
                except Exception as e:
                        logging.exception(f"Unable to connect to {info['host']} on port {info['port']}: {e}")
      
                return sft

        def download_remote_dir(
                        self, 
                        sft, 
                        local_dir, 
                        remote_dir
                        ):
                """Recursively download a directory from an SFTP server."""
                # Ensure the local directory exists
                os.makedirs(local_dir, exist_ok=True)

                # List files and directories in the remote directory
                for entry in sft.listdir_attr(remote_dir):
                        remote_path = remote_dir + '/' + entry.filename
                        local_path = os.path.join(local_dir, entry.filename)

                        if S_ISDIR(entry.st_mode):
                                self.download_remote_dir(local_path, remote_path)
                                logging.INFO(f"DOWNLOAD DIR: {remote_dir}")
                        else:                      
                                sft.get(remote_path, local_path)
                                logging.INFO(f"DOWNLOAD FILE: {remote_dir}")
                return

        def upload_local_dir(
                        self,
                        sft, 
                        local_dir: str, 
                        remote_dir:str
                        ):
                """upload file from directory"""

                for item in os.listdir(local_dir):
                        local_path = os.path.join(local_dir, item)
                        remote_path = remote_dir+ "/" + item

                        if os.path.isfile(local_path):
                                # Upload file
                                sft.put(local_path, remote_path)
                                logging.INFO(f"UPLOAD DIR: {local_dir}")

                        else:
                                # Create remote directory if it doesn't exist
                                try:
                                        sft.stat(remote_path)
                                except FileNotFoundError as err:
                                        logging.INFO(f"FILE CREATED:{err}")
                                        sft.mkdir(remote_path)

                                self.upload_local_dir(local_path, remote_path)
                return
        
        def upload_download_file(
                                self,
                                sft, 
                                option: str, 
                                local_filepth: str, 
                                remote_filepth: str
                                 ):
                """Upload or download single file from the server
                Args:
                    option (str): get or push
                """ 
                assert isinstance(option, str), "option must be string type"
                assert (os.path.basename(local_filepth) == os.path.basename(remote_filepth)), "file name must be same"                

                option = option.lower()
                
                if option == "get":
                        sft.get(remote_filepth, local_filepth)
                elif option == "push":
                        assert os.path.isfile(local_filepth), "local_filepth must be file"
                        try:
                                sft.put(local_filepth, remote_filepth)
                                # logging.INFO(f"File uploaded:{local_filepth}")
                        except FileNotFoundError as err:
                                logging.exception(f"File not uploaded:{err}")
                        else:
                                logging.exception("remote_filepth not found")
                else:
                        logging.exception("Invalid option") 
                return
        
        def listdir_r(
                self,
                sft, 
                remotedir:str, 
                store_pths: list
                 )-> list:
                """recursively loop through the folder"""
                
                store_pths = []
                logging.INFO("Listed directories")
                for entry in sft.listdir_attr(remotedir):
                        remotepath = remotedir + "/" + entry.filename
                        mode = entry.st_mode
                        if S_ISDIR(mode):
                                fold, ext = os.path.split(remotepath)
                                self.listdir_r(remotepath)
                                store_pths.append((ext, fold)) 
                        else:
                                pass
                return store_pths


if __name__ == "__main__":
        sftp_info = {
        "site" :{
            "host":"xx.xx.xx.xx",
            "user":"xxxx",
            "port":xx,
            "password":"xoxo",
            },
        "upload":{
            "local_dir": "/home/",
            "remote_dir": "/home",

            },
        "download":{
            "local_dir":  "/home/",
            "remote_dir":"/home/",
        },
        "file_up":{
        }
    }
        
        
        #grab connection
        sftp_conn = Mysftp(sftp_info)
        server = sftp_conn.connect_sftp()
        upload   = False
        download = False

        if upload:
                sftp_conn.upload_local_dir(
                        sft = server,
                        local_dir=sftp_info["upload"]["local_dir"],
                        remote_dir=sftp_info["upload"]["remote_dir"])
        if download:
                sftp_conn.download_remote_dir(
                        sft=server,
                        local_dir=sftp_info["download"]["local_dir"],
                        remote_dir=sftp_info["download"]["remote_dir"])
        
        #option set/push
        sftp_conn.upload_download_file(
                        sft=server,
                        option="push", 
                        local_filepth="mc3.mov", 
                        remote_filepth="mc3.mov",
                                )
