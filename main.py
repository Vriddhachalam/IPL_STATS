import urllib.request
import os
import zipfile
from tools import download_progress
from tools import zip_extract_perc
from flattener import flatten
from stats_2023 import charts
import http.server
import socketserver
import threading
import webbrowser
import time


zip_file="ipl_json.zip"

class StoppableHTTPServer(socketserver.TCPServer):
    def __init__(self, server_address, handler_class):
        super().__init__(server_address, handler_class)
        self._stop_event = threading.Event()

    def serve_forever(self):
        while not self._stop_event.is_set():
            self.handle_request()

    def stop(self):
        self._stop_event.set()

def start_http_server(port=8888):
    handler = http.server.SimpleHTTPRequestHandler
    # with socketserver.TCPServer(("0.0.0.0", port), handler) as httpd:
    httpd = StoppableHTTPServer(("0.0.0.0", port), handler)
    print(f"\nServing HTTP on http://localhost:{port}/template_2023 ")
    print(f"CTRL + click the above link")
    httpd.serve_forever()

    # Add a delay before opening the browser   Sometimes, adding a slight delay before opening the browser can help, especially if there's a race condition between starting the server and opening the browser.
    time.sleep(2)
    try:
        webbrowser.open(f"http://localhost:{port}/template_2023")
        httpd.serve_forever()
        
    except KeyboardInterrupt:
        print("\nShutting down the server...")
        httpd.stop()
        httpd.server_close()
        print("Server stopped.")


def zip_extract(extracted_size,url,retries,zip_file):
    if retries > 3:
        print("Exceeded maximum number of retries.")
        return

    try:
        # Extract the JSON files from the downloaded zip file
        with zipfile.ZipFile(os.path.join(os.getcwd(), zip_file), "r") as zip_ref:

            total_size = sum(file_info.file_size for file_info in zip_ref.infolist())

            for file_info in zip_ref.infolist():
                extracted_size += int(file_info.file_size)
                zip_extract_perc(zip_file,extracted_size, total_size)    
                zip_ref.extract(file_info.filename, all_json_directory)

    except zipfile.BadZipFile:

        urllib.request.urlretrieve(url, os.path.join(os.getcwd(), zip_file), reporthook=download_progress)
        zip_extract(extracted_size,url,retries+1,zip_file=zip_file)       
        
    # Print a message indicating that the update has been downloaded and extracted
    print("\nALL JSON files have been extracted successfully!")    




all_json_directory = os.path.join(os.getcwd(), zip_file.split('.')[0])

# try:
#     os.mkdir(all_json_directory)
#     print("\nFolder created successfully!")
# except FileExistsError:
#     print("Folder already exists!")
# except Exception as e:
#     print("An error occurred:", str(e))

url = "https://cricsheet.org/"

if os.path.exists(os.getcwd()+"\\"+zip_file) == True:
    print("\nThe file "+ os.getcwd()+"\\"+zip_file+" exists in the directory.")
else:
    urllib.request.urlretrieve(url+"downloads/"+zip_file, os.path.join(os.getcwd(), zip_file), reporthook=download_progress)
    print(f"\nDownloaded  {zip_file} successfully!")


if os.path.exists(os.getcwd()+"\\"+zip_file.split('.zip')[0]) == True:
    print("\nThe folder "+ os.getcwd()+"\\"+zip_file.split('.zip')[0]+" exists in the directory.")
else:
    extracted_size = 0
    zip_extract(extracted_size,url,retries=0,zip_file=zip_file)

flatten()

charts()

server_thread = threading.Thread(target=start_http_server)
server_thread.daemon = True  # Set the thread as a daemon so it will exit when the main program exits
server_thread.start()
# You can add other tasks or code that should run alongside the HTTP server
try:
    while True:
        pass  # Keep the main thread running
except KeyboardInterrupt:
    print("\nMain program interrupted. Exiting...")

# if sys.argv='only'
# Start the HTTP server in a separate thread
# server_thread = threading.Thread(target=start_http_server)
# server_thread.start()
