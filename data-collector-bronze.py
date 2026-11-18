import psutil, time, csv, boto3, os
from datetime import datetime
from getmac import get_mac_address
from io import StringIO
from dotenv import load_dotenv

mac_address = get_mac_address().upper().replace(':', "_")

def capture():
    user = mac_address
    last_net = psutil.net_io_counters()
    last_time = time.time()
    
    for i in range(5):
        current_time = time.time()
        time_delta = current_time - last_time
        current_net = psutil.net_io_counters()
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_frequency =psutil.cpu_freq().current
        ram_percent = psutil.virtual_memory().percent
        swap_memory_percent = psutil.swap_memory().percent 
        processes_qtd = psutil.pids()
        processes_zombies_qtd =0

        for proc in psutil.process_iter(['status']):
            try:
                if proc.info['status']== psutil.STATUS_ZOMBIE:
                    processes_zombies_qtd += 1

            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue

        processes_zombies_percent = processes_zombies_qtd


        try:
            bytes_sent_delta = current_net.bytes_sent - last_net.bytes_sent
            upload_speed = (bytes_sent_delta * 8) / (time_delta * 1_000_000)
        except Exception:
            upload_speed = 0.0
        

       
        try:
            bytes_recv_delta = current_net.bytes_recv - last_net.bytes_recv
            download_speed =(bytes_recv_delta * 8) / (time_delta * 1_000_000)
        except Exception:
            download_speed = 0.0

        
        last_net = current_net
        last_time = current_time

        disk = psutil.disk_usage('/').free 


        timestamp = datetime.now()

        exhibit([user, cpu_percent, cpu_frequency, ram_percent, swap_memory_percent, processes_qtd, processes_zombies_percent,upload_speed, download_speed, disk, timestamp])
        store([user, cpu_percent, cpu_frequency, ram_percent, swap_memory_percent,processes_qtd,processes_zombies_percent, upload_speed, download_speed, disk, timestamp])

        time.sleep(9)
        
    save(f'{mac_address}.csv')

def exhibit(data):
    line_user = f"Endereço MAC do dispositivo: {data[0]}"
    line_cpu_percent  = f"Uso atual da CPU: {data[1]}%"
    line_cpu_frequency  = f"Frequência atual da CPU: {data[2]} GHz"
    line_ram_percent  = f"Uso atual de memória RAM: {data[3]}%"
    line_swap_memory_percent = f"Uso atual da memória swap: {data[4]}%"
    line_processes_qtd = f"Quantidade atual de processos ativos: {data[5]}"
    line_processes_zombies_percent = f"Porcentagem de processos zumbis dentre o total de processos: {data[6]}%"
    line_upload_bytes = f"Tráfego atual de upload de bytes da rede: {data[7]} Mbps"
    line_download_bytes = f"Tráfego atual de download de bytes da rede: {data[8]} Mbps"
    line_disk = f"Espaço livre em disco: {data[9]} GiB"
    line_timestamp = f"Momento de captura: {data[10].strftime('%Y-%m-%d %H:%M:%S')}"

    print(f"""
    ----------------------------------------------------------------
    | {line_user:<60} |
    | {line_cpu_percent:<60} |
    | {line_cpu_frequency:<60} |
    | {line_ram_percent:<60} |
    | {line_swap_memory_percent:<60} |
    | {line_processes_qtd:<60} |
    | {line_processes_zombies_percent:<60} |
    | {line_upload_bytes:<60} |
    | {line_download_bytes:<60} |
    | {line_disk:<60} |
    | {line_timestamp:<60} |
    ----------------------------------------------------------------
    """)

def store(data):
    with open(f'{mac_address}.csv', 'a', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow([data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8], data[9], data[10]])

with open(f'{mac_address}.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile, delimiter=';')
        writer.writerow(["user", "cpu_percent", "cpu_frequency", "ram_percent", "swap_memory_percent", "processes_qtd", "processes_zombies_percent", "upload_speed", "download_speed", "disk", "timestamp"])

def save(file):
    load_dotenv()

    session = boto3.Session(
        aws_access_key_id=os.getenv("aws_access_key_id"),
        aws_secret_access_key=os.getenv("aws_secret_access_key"),
        aws_session_token=os.getenv("aws_session_token"),
        region_name="us-east-1",
    )

    s3_client = session.client("s3")
    bucket_name = "microhub-project-bucket-lucas"
    
    csv_buffer = StringIO()
    
    file_name = f"01-raw-bronze/{file}"
    s3_client.put_object(Bucket=bucket_name, Key=file_name, Body=csv_buffer.getvalue())

capture()