# ------------------------------------------------------------------------------
# Written by Mukesh Narendran (mukesh.narendran@gmail.com)
# Connects with sftp server, upload, download folders & files
# Add db
alla
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
                print("log folder already present")
        
        # open logger
        nm = datetime.now().strftime("%d/%m/%Y %H:%M:%S").replace(' ', '_').replace('/', '_')
        log_file = os.path.join(fold, nm+".log")    
        return log_file

# Create log file
log_fil = create_log()
logging.basicConfig(
                filename=log_fil,
                format='%(asctime)s:%(levelname)s:%(message)s',
                level=logging.INFO)

class Bovisftp:
        def __init__(self, params: dict):
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

                        logging.info(f"Connected has been established with {info['host']}:{info['port']}")
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
                        local_dir: str, 
                        remote_dir: str,
                        is_base:bool
                        ) -> None:
                """Download directory from the server
                Args:
                    sft (_type_): sftp connection
                    local_dir (str): youir local path to download
                    remote_dir (str): server path to download
                    is_base (bool): if True, then it will create a folder with the same name as remote_dir
                """                             
                # Ensure the local directory exists
                if is_base:
                    local_dir = os.path.join(local_dir, os.path.basename(remote_dir))
                    os.makedirs(local_dir, exist_ok=True)

                # List files and directories in the remote directory
                for entry in sft.listdir_attr(remote_dir):
                        remote_path = remote_dir + '/' + entry.filename
                        local_path = os.path.join(local_dir, entry.filename)
                        if S_ISDIR(entry.st_mode):
                                self.download_remote_dir(
                                        sft, 
                                        local_path, 
                                        remote_path,
                                        is_base=False
                                        )
                                logging.info(f"DOWNLOAD DIR: {remote_path}")
                        else:                   
                                os.makedirs(os.path.dirname(local_path), exist_ok=True)   
                                sft.get(remote_path, local_path)
                                logging.info(f"DOWNLOAD FILE: {remote_path}")
                return

        def upload_local_dir(
                        self,
                        sft, 
                        local_dir: str, 
                        remote_dir:str
                        )-> None:
                """Recursively upload a directory to an SFTP server

                Args:
                    sft (_type_): sftp connection
                    local_dir (str): you local path to upload
                    remote_dir (str): server path to upload
                """                

                for item in os.listdir(local_dir):
                        local_path = os.path.join(local_dir, item)
                        remote_path = remote_dir+ "/" + item

                        if os.path.isfile(local_path):
                                # Upload file
                                sft.put(local_path, remote_path)
                                logging.info(f"UPLOAD DIR: {local_dir}")

                        else:
                                # Create remote directory if it doesn't exist
                                try:
                                        sft.stat(remote_path)
                                except FileNotFoundError as err:
                                        logging.info(f"FILE CREATED ON SERVER:{err}")
                                        sft.mkdir(remote_path)

                                self.upload_local_dir(sft, local_path, remote_path)
                return
        
        def upload_download_file(
                                self,
                                sft, 
                                option: str, 
                                local_filepth: str, 
                                remote_filepth: str
                                 ) -> None:
                """Upload or download single file from the server
                Args:
                    option (str): get or push
                """ 
                assert isinstance(option, str), "option must be string type"
                option = option.lower()
                
                if option == "get":
                        # local_filepth = os.path.join(local_filepth, os.path.basename(remote_filepth))
                        sft.get(remote_filepth, local_filepth)
                elif option == "push":
                        assert os.path.isfile(local_filepth), "local_filepth must be file"

                        remote_filepth = os.path.join(remote_filepth, os.path.basename(local_filepth))
                        try:
                                sft.put(local_filepth, remote_filepth)
                                logging.info(f"File UPLOADED: {local_filepth}")
                        except FileNotFoundError as err:
                                logging.exception(f"FILE NOT UPLOADED:{err}")
                        else:
                                logging.exception(f"REMOTE PATH NOT FOUND: {remote_filepth}")
                else:
                        logging.exception("INVALID OPTION") 
                return
        
        def listdir_r(
                self,
                sft, 
                remotedir:str, 
                 )-> list:
                """List all the directories in the server

                Args:
                    sft (_type_): sftp connection
                    remotedir (str): remote directory

                Returns:
                    list: filepaths within directory
                """                
                
                store_pths = []
                logging.info("Listing all the directories")
                for entry in sft.listdir_attr(remotedir):
                        remotepath = remotedir + "/" + entry.filename
                        mode = entry.st_mode
                        if S_ISDIR(mode):
                                fold, ext = os.path.split(remotepath)
                                self.listdir_r(sft, remotepath)
                                store_pths.append((ext, fold)) 
                        else:
                                pass
                return store_pths
if __name__ == "__main__":
    print("NA")
