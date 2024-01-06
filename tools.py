import sys
import time
import urllib

def download_progress(count, block_size, total_size):
    global start_time
    if count == 0:
        start_time = time.time()
        return
    duration = time.time() - start_time
    progress_size = int(count * block_size)
    speed = int(progress_size / (1024 * duration))
    percent = int(count * block_size * 100 / total_size)
    sys.stdout.write("\r\nDownloading zipfile ...%d%%, %d MB, %d KB/s, %d seconds passed\n" %
                    (percent, progress_size / (1024 * 1024), speed, duration))
    sys.stdout.flush()


def zip_extract_perc(zip_file,extracted_size, total_size):
    """Simulate extraction progress based on file sizes."""

    percent = int((extracted_size / total_size) * 100)
    # print(f"Extracting... {percent}% complete   extracted_size:{extracted_size}, total_size:{total_size} ", end='\r')
    sys.stdout.write("\r\nExtracting %s ... %d%%, %d B, Total_size: %d B\n" %
                    (zip_file,percent, extracted_size, total_size))
    sys.stdout.flush()