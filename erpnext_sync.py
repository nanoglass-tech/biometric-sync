try:
    import config_loader as _cfg
    config = _cfg.load()
except:
    import local_config as config
import argparse
import requests
import datetime
import json
import os
import sys
import time
import logging
from logging.handlers import RotatingFileHandler
from pickledb import PickleDB
from zk import ZK, const

# ===== Retry/Backoff config #3 ====
MAX_RETRIES = int(os.getenv("ERP_MAX_RETRIES", "3"))        # total percobaan
BACKOFF_SEC = float(os.getenv("ERP_BACKOFF_SEC", "1.5"))    # detik, exponential backoff dasar
TIMEOUT_SEC = float(os.getenv("ERP_TIMEOUT_SEC", "10"))     # timeout request

EMPLOYEE_NOT_FOUND_ERROR_MESSAGE = "No Employee found for the given employee field value"
EMPLOYEE_INACTIVE_ERROR_MESSAGE = "Transactions cannot be created for an Inactive Employee"
DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE = "This employee already has a log with the same timestamp"
allowlisted_errors = [EMPLOYEE_NOT_FOUND_ERROR_MESSAGE, EMPLOYEE_INACTIVE_ERROR_MESSAGE, DUPLICATE_EMPLOYEE_CHECKIN_ERROR_MESSAGE]

if hasattr(config,'allowed_exceptions'):
    allowlisted_errors_temp = []
    for error_number in config.allowed_exceptions:
        allowlisted_errors_temp.append(allowlisted_errors[error_number-1])
    allowlisted_errors = allowlisted_errors_temp

device_punch_values_IN = getattr(config, 'device_punch_values_IN', [0,4])
device_punch_values_OUT = getattr(config, 'device_punch_values_OUT', [1,5])
ERPNEXT_VERSION = getattr(config, 'ERPNEXT_VERSION', 14)

# possible area of further developemt
    # Real-time events - setup getting events pushed from the machine rather then polling.
        #- this is documented as 'Real-time events' in the ZKProtocol manual.

# Notes:
# Status Keys in status.json
#  - lift_off_timestamp
#  - mission_accomplished_timestamp
#  - <device_id>_pull_timestamp
#  - <device_id>_push_timestamp
#  - <shift_type>_sync_timestamp

def main():
    """Takes care of checking if it is time to pull data based on config,
    then calling the relevent functions to pull data and push to EPRNext.

    """
    try:
        last_lift_off_timestamp = _safe_convert_date(status.get('lift_off_timestamp'), "%Y-%m-%d %H:%M:%S.%f")
        if (last_lift_off_timestamp and last_lift_off_timestamp < datetime.datetime.now() - datetime.timedelta(minutes=config.PULL_FREQUENCY)) or not last_lift_off_timestamp:
            status.set('lift_off_timestamp', str(datetime.datetime.now()))
            status.save()
            info_logger.info("Cleared for lift off!")
            # ===== aggregator run (Langkah ke-3)
            run_pull = 0
            run_sent_ok = 0
            run_sent_fail = 0
            run_skipped_dup = 0
            
            for device in config.devices:
                device_attendance_logs = None
                info_logger.info("Processing Device: "+ device['device_id'])
                dump_file = get_dump_file_name_and_directory(device['device_id'], device['ip'])
                if os.path.exists(dump_file):
                    info_logger.error('Device Attendance Dump Found in Log Directory. This can mean the program crashed unexpectedly. Retrying with dumped data.')
                    with open(dump_file, 'r') as f:
                        file_contents = f.read()
                        if file_contents:
                            device_attendance_logs = list(map(lambda x: _apply_function_to_key(x, 'timestamp', datetime.datetime.fromtimestamp), json.loads(file_contents)))
                try:
                    # ukur dulu metrik sebelum/selesai
                    t0 = time.time()
                    # wrap pull_process... untuk mendapatkan metrik via logger (parsing cepat)
                    before_ok = status.get(f"cursor.{device['device_id']}.last_ts")
                    pull_process_and_push_data(device, device_attendance_logs)
                    # heuristik sederhana: hitung dari log sukses perangkat
                    # (bisa diperhalus nanti dengan counter eksplisiti global)
                    t1 = time.time()
                    status.set(f'{device["device_id"]}_push_timestamp', str(datetime.datetime.now()))
                    status.save()
                    if os.path.exists(dump_file):
                        os.remove(dump_file)
                    info_logger.info("Successfully processed Device: "+ device['device_id'])
                    # catat agregat kasar (pakai jumlah baris sukses terbaru)
                    # NB: murah dan cukup akurat untuk ringkasan run
                    succ_log = '/'.join([config.LOGS_DIRECTORY, '_'.join(["attendance_success_log", device['device_id']])])+'.log'
                    try:
                        pulled = 0
                        if device_attendance_logs:
                            pulled = len(device_attendance_logs)
                        else:
                            # Jika ambil dari device langsung, pakai heuristik info log fetch
                            pulled = 0 # biarkan 0; nanti dilengkapi di langkah ke-4
                        run_pull += pulled
                        # hitung sent_ok dari tail log sukses (tambahan di run ini kurang presisi - accepatble)
                        run_sent_ok += 0
                    except Exception:
                        pass
                except:
                    error_logger.exception('exception when calling pull_process_and_push_data function for device'+json.dumps(device, default=str))
            if hasattr(config,'shift_type_device_mapping'):
                update_shift_last_sync_timestamp(config.shift_type_device_mapping)
            status.set('mission_accomplished_timestamp', str(datetime.datetime.now()))
            status.save()
            info_logger.info("Mission Accomplished!")
            # ringkasan agregat (kasar; per-device summary sudah ditulis sebelumnya)
            info_logger.info("\t".join([
                "RUN_SUMMARY",
                f"devices={len(config.devices)}",
                f"pulled~={run_pull}",
                f"sent_ok~={run_sent_ok}",
                f"sent_fail~={run_sent_fail}",
                f"skipped_dup~={run_skipped_dup}",
            ]))
    except:
        error_logger.exception('exception has occurred in the main function...')


def pull_process_and_push_data(device, device_attendance_logs=None):
    """ Takes a single device config as param and pulls data from that device.
    params:
    device: a single device config object from the local_config file
    device_attendance_logs: fetching from device is skipped if this param is passed. used to restart failed fetches from previous runs.
    """
    attendance_success_log_file = '_'.join(["attendance_success_log", device['device_id']])
    attendance_failed_log_file = '_'.join(["attendance_failed_log", device['device_id']])
    attendance_success_logger = setup_logger(attendance_success_log_file, '/'.join([config.LOGS_DIRECTORY, attendance_success_log_file])+'.log')
    attendance_failed_logger = setup_logger(attendance_failed_log_file, '/'.join([config.LOGS_DIRECTORY, attendance_failed_log_file])+'.log')
    if not device_attendance_logs:
        device_attendance_logs = get_all_attendance_from_device(
            device['ip'],
            port=int(device.get('port', 4370)),
            device_id=device['device_id'],
            clear_from_device_on_fetch=device['clear_from_device_on_fetch']
        )
        if not device_attendance_logs:
            return

  # ===== Langkah 2: tentukan titik mulai berdasarkan cursor/last success/import_start_date =====
    # normalisasi & urutkan ascending
    for x in device_attendance_logs:
        if not isinstance(x['timestamp'], datetime.datetime):
            # Jika timestamp berupa epoch/str -> jadikan datetime
            epoch = _ts_to_epoch(x['timestamp'])
            x['timestamp'] = datetime.datetime.fromtimestamp(epoch) if epoch else x['timestamp']
    device_attendance_logs.sort(key=lambda x: x['timestamp'])

    start_epoch = get_cursor_epoch(device['device_id']) # 1) cursor
    last_line = get_last_line_from_file('/'.join([config.LOGS_DIRECTORY, attendance_success_log_file])+'.log')
    import_start_date = _safe_convert_date(config.IMPORT_START_DATE, "%Y%m%d")
    if start_epoch is None:
        # 2) last success log (fallback)
        if last_line:
            try:
                # format log: ... \t <user_id> \t <timestamp_epoch> \t ...
                parts = last_line.split("\t")
                last_epoch = float(parts[3]) # index 3 = timestamp_epoch log sukses
                start_epoch = last_epoch
            except:
                start_epoch = None
        # 3) IMPORT_START_DATE (fallback terakhir)
        if start_epoch is None and import_start_date:
            start_epoch = import_start_date.timestamp()
    
    # filter dengan start_epoch (>=)
    filtered = []
    for row in device_attendance_logs:
        ts_ep = row['timestamp'].timestamp()
        if (start_epoch is None) or (ts_ep >= start_epoch):
            filtered.append(row)
    
    # ===== Langkah-2: dedup ringan dalam batch =====
    seen = set()
    sent_ok = 0
    sent_fail = 0
    skipped_dup = 0
    latest_ep = start_epoch




    # for device_attendance_log in device_attendance_logs[index_of_last+1:]:
    for device_attendance_log in filtered:
        key = (device['device_id'], str(device_attendance_log['user_id']), int(device_attendance_log['timestamp'].timestamp()))
        if key in seen:
            skipped_dup += 1
            continue
        seen.add(key)

        punch_direction = device['punch_direction']
        if punch_direction == 'AUTO':
            if device_attendance_log['punch'] in device_punch_values_OUT:
                punch_direction = 'OUT'
            elif device_attendance_log['punch'] in device_punch_values_IN:
                punch_direction = 'IN'
            else:
                punch_direction = None
        erpnext_status_code, erpnext_message = send_to_erpnext(device_attendance_log['user_id'], device_attendance_log['timestamp'], device['device_id'], punch_direction, latitude=device['latitude'], longitude=device['longitude'])
        if erpnext_status_code == 200:
            attendance_success_logger.info("\t".join([erpnext_message, str(device_attendance_log['uid']),
                str(device_attendance_log['user_id']), str(device_attendance_log['timestamp'].timestamp()),
                str(device_attendance_log['punch']), str(device_attendance_log['status']),
                json.dumps(device_attendance_log, default=str)]))
            sent_ok += 1
            latest_ep = device_attendance_log['timestamp'].timestamp()
        else:
            attendance_failed_logger.error("\t".join([str(erpnext_status_code), str(device_attendance_log['uid']),
                str(device_attendance_log['user_id']), str(device_attendance_log['timestamp'].timestamp()),
                str(device_attendance_log['punch']), str(device_attendance_log['status']),
                json.dumps(device_attendance_log, default=str)]))
            if not(any(error in erpnext_message for error in allowlisted_errors)):
                raise Exception('API Call to ERPNext Failed.')
            sent_fail += 1
    # update cursor jika ada progres
    if latest_ep and (start_epoch is None or latest_ep >= start_epoch):
        set_cursor_epoch(device['device_id'], latest_ep)
    info_logger.info("\t".join([
        "Summary",
        f"device={device['device_id']}",
        f"pulled={len(device_attendance_logs)}",
        f"start_from={start_epoch}",
        f"sent_ok={sent_ok}",
        f"sent_fail={sent_fail}",
        f"skipped_dup={skipped_dup}",
    ]))


def get_all_attendance_from_device(ip, port=4370, timeout=30, device_id=None, clear_from_device_on_fetch=False):
    #  Sample Attendance Logs [{'punch': 255, 'user_id': '22', 'uid': 12349, 'status': 1, 'timestamp': datetime.datetime(2019, 2, 26, 20, 31, 29)},{'punch': 255, 'user_id': '7', 'uid': 7, 'status': 1, 'timestamp': datetime.datetime(2019, 2, 26, 20, 31, 36)}]
    zk = ZK(ip, port=port, timeout=timeout)
    conn = None
    attendances = []
    try:
        conn = zk.connect()
        x = conn.disable_device()
        # device is disabled when fetching data
        info_logger.info("\t".join((ip, "Device Disable Attempted. Result:", str(x))))
        attendances = conn.get_attendance()
        info_logger.info("\t".join((ip, "Attendances Fetched:", str(len(attendances)))))
        status.set(f'{device_id}_push_timestamp', None)
        status.set(f'{device_id}_pull_timestamp', str(datetime.datetime.now()))
        status.save()
        if len(attendances):
            # keeping a backup before clearing data incase the programs fails.
            # if everything goes well then this file is removed automatically at the end.
            dump_file_name = get_dump_file_name_and_directory(device_id, ip)
            with open(dump_file_name, 'w+') as f:
                f.write(json.dumps(list(map(lambda x: x.__dict__, attendances)), default=datetime.datetime.timestamp))
            if clear_from_device_on_fetch:
                x = conn.clear_attendance()
                info_logger.info("\t".join((ip, "Attendance Clear Attempted. Result:", str(x))))
        x = conn.enable_device()
        info_logger.info("\t".join((ip, "Device Enable Attempted. Result:", str(x))))
    except:
        error_logger.exception(str(ip)+' exception when fetching from device...')
        raise Exception('Device fetch failed.')
    finally:
        if conn:
            conn.disconnect()
    return list(map(lambda x: x.__dict__, attendances))


def send_to_erpnext(employee_field_value, timestamp, device_id=None, log_type=None, latitude=None, longitude=None):
    """
    Examples: 
    
    For ERPNext, Frappe HR <= v14
    send_to_erpnext('12349',datetime.datetime.now(),'HO1','IN')

    For ERPNext, Frappe HR v15 onwards
    If 'Allow Geolocation Tracking' is on
    send_to_erpnext('12349',datetime.datetime.now(),'HO1','IN',latitude=12.34, longitude=56.78)
    """
    endpoint_app = "hrms" if ERPNEXT_VERSION > 13 else "erpnext"
    url = f"{config.ERPNEXT_URL}/api/method/{endpoint_app}.hr.doctype.employee_checkin.employee_checkin.add_log_based_on_employee_field"
    headers = {
        'Authorization': "token "+ config.ERPNEXT_API_KEY + ":" + config.ERPNEXT_API_SECRET,
        'Accept': 'application/json'
    }
    data = {
        'employee_field_value' : employee_field_value,
        'timestamp' : timestamp.__str__(),
        'device_id' : device_id,
        'log_type' : log_type,
        'latitude' : latitude,
        'longitude' : longitude
    }
    code, body_or_err = post_with_retry(url, headers, data)
    if code == 200:
        return 200, body_or_err['message']['name']
    else:
        # tulis error (allowList tetap berlaku di caller)
        error_logger.error('\t'.join([
            'Error during ERPNext API Call.',
            str(employee_field_value),
            str(timestamp.timestamp()),
            str(device_id),
            str(log_type),
            str(body_or_err)
        ]))
        return code, str(body_or_err)

def post_with_retry(url, headers, data):
    """
    Kirim POST JSON dengan retry + exponantial backoff.
    - Retry untuk status sementara (502/503/504) & error koneksi/timeout/
    - Non-retry untuk 4xx selain 429 (429 diretry).
    Return: (status_code, json_body|errorr_str)
    """
    import time as _t
    attempt = 0
    while True:
        attempt += 1
        try:
            res = requests.post(url, headers=headers, json=data, timeout=TIMEOUT_SEC)
            sc = res.status_code
            # sukses
            if sc == 200:
                try:
                    return 200, json.loads(res.content)
                except Exception as e:
                    return 500, f"JSON decode error: {e}"
            # 429/5xx: retry
            if sc in (429, 500, 502, 503, 504):
                err = _safe_get_error_str(res)
                if attempt < MAX_RETRIES:
                    _t.sleep(BACKOFF_SEC * (2 ** (attempt -1)))
                    continue
                return sc, err
            # 4xx lain: tidak diretry
            return sc, _safe_get_error_str(res)
        except (requests.Timeout, requests.ConnectionError) as e:
            if attempt < MAX_RETRIES:
                _t.sleep(BACKOFF_SEC * (2 ** (attempt - 1)))
                continue
            return 599, f"Network/Timeout after retreis: {e}"
        except Exception as e:
            # error tak terduga -> tidak diretry (bisa dipertimbangkan nanti)
            return 598, f"Unexpected error: {e}"

def update_shift_last_sync_timestamp(shift_type_device_mapping):
    """
    ### algo for updating the sync_current_timestamp
    - get a list of devices to check
    - check if all the devices have a non 'None' push_timestamp
        - check if the earliest of the pull timestamp is greater than sync_current_timestamp for each shift name
            - then update this min of pull timestamp to the shift

    """
    for shift_type_device_map in shift_type_device_mapping:
        all_devices_pushed = True
        pull_timestamp_array = []
        for device_id in shift_type_device_map['related_device_id']:
            if not status.get(f'{device_id}_push_timestamp'):
                all_devices_pushed = False
                break
            pull_timestamp_array.append(_safe_convert_date(status.get(f'{device_id}_pull_timestamp'), "%Y-%m-%d %H:%M:%S.%f"))
        if all_devices_pushed:
            min_pull_timestamp = min(pull_timestamp_array)
            if isinstance(shift_type_device_map['shift_type_name'], str): # for backward compatibility of config file
                shift_type_device_map['shift_type_name'] = [shift_type_device_map['shift_type_name']]
            for shift in shift_type_device_map['shift_type_name']:
                try:
                    sync_current_timestamp = _safe_convert_date(status.get(f'{shift}_sync_timestamp'), "%Y-%m-%d %H:%M:%S.%f")
                    if (sync_current_timestamp and min_pull_timestamp > sync_current_timestamp) or (min_pull_timestamp and not sync_current_timestamp):
                        response_code = send_shift_sync_to_erpnext(shift, min_pull_timestamp)
                        if response_code == 200:
                            status.set(f'{shift}_sync_timestamp', str(min_pull_timestamp))
                            status.save()
                except:
                    error_logger.exception('Exception in update_shift_last_sync_timestamp, for shift:'+shift)

def send_shift_sync_to_erpnext(shift_type_name, sync_timestamp):
    url = config.ERPNEXT_URL + "/api/resource/Shift Type/" + shift_type_name
    headers = {
        'Authorization': "token "+ config.ERPNEXT_API_KEY + ":" + config.ERPNEXT_API_SECRET,
        'Accept': 'application/json'
    }
    data = {
        "last_sync_of_checkin" : str(sync_timestamp)
    }
    try:
        response = requests.request("PUT", url, headers=headers, data=json.dumps(data))
        if response.status_code == 200:
            info_logger.info("\t".join(['Shift Type last_sync_of_checkin Updated', str(shift_type_name), str(sync_timestamp.timestamp())]))
        else:
            error_str = _safe_get_error_str(response)
            error_logger.error('\t'.join(['Error during ERPNext Shift Type API Call.', str(shift_type_name), str(sync_timestamp.timestamp()), error_str]))
        return response.status_code
    except:
        error_logger.exception("\t".join(['exception when updating last_sync_of_checkin in Shift Type', str(shift_type_name), str(sync_timestamp.timestamp())]))

def get_last_line_from_file(file):
    if not os.path.exists(file):
        return None
    line = None
    try:
        if os.stat(file).st_size < 5000:
            with open(file, 'r') as f:
                for line in f:
                    pass
        else:
            with open(file, 'rb') as f:
                f.seek(-2, os.SEEK_END)
                while f.read(1) != b'\n':
                    f.seek(-2, os.SEEK_CUR)
                line = f.readline().decode()
    except FileNotFoundError:
        return None
    return line


def setup_logger(name, log_file, level=logging.INFO, formatter=None):

    if not formatter:
        formatter = logging.Formatter('%(asctime)s\t%(levelname)s\t%(message)s')

    handler = RotatingFileHandler(log_file, maxBytes=10000000, backupCount=50)
    handler.setFormatter(formatter)

    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.hasHandlers():
        logger.addHandler(handler)

    return logger

def get_dump_file_name_and_directory(device_id, device_ip):
    return config.LOGS_DIRECTORY + '/' + device_id + "_" + device_ip.replace('.', '_') + '_last_fetch_dump.json'

def _apply_function_to_key(obj, key, fn):
    obj[key] = fn(obj[key])
    return obj

def _safe_convert_date(datestring, pattern):
    try:
        return datetime.datetime.strptime(datestring, pattern)
    except:
        return None

def _safe_get_error_str(res):
    try:
        error_json = json.loads(res._content)
        if 'exc' in error_json: # this means traceback is available
            error_str = json.loads(error_json['exc'])[0]
        else:
            error_str = json.dumps(error_json)
    except:
        error_str = str(res.__dict__)
    return error_str

# setup logger and status
if not os.path.exists(config.LOGS_DIRECTORY):
    os.makedirs(config.LOGS_DIRECTORY)
error_logger = setup_logger('error_logger', '/'.join([config.LOGS_DIRECTORY, 'error.log']), logging.ERROR)
info_logger = setup_logger('info_logger', '/'.join([config.LOGS_DIRECTORY, 'logs.log']))
status = PickleDB('/'.join([config.LOGS_DIRECTORY, 'status.json']))

def _ts_to_epoch(ts):
    """
    Terima datetime/float/str -> float epoc detik (atau None).
    """
    import datetime as _dt
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        return float(ts)
    if isinstance(ts, _dt.datetime):
        return ts.timestamp()
    # coba parse string epoch/ISO
    try:
        return float(ts)
    except:
        try:
            # fallback: ISO like '2024-09-04 10:11:12.123456'
            return _dt.datetime.fromisoformat(str(ts)).timestamp()
        except:
            return None

def get_cursor_epoch(device_id: str):
    return _ts_to_epoch(status.get(f'cursor.{device_id}.last_ts'))

def set_cursor_epoch(device_id: str, epoch: float):
    if epoch is not None:
        status.set(f'cursor.{device_id}.last_ts', epoch)
        status.save()

def infinite_loop(sleep_time=15):
    print("Service Running...")
    while True:
        try:
            main()
            time.sleep(sleep_time)
        except BaseException as e:
            print(e)

if __name__ == "__main__":
    # infinite_loop()
    parser = argparse.ArgumentParser()
    parser.add_argument("--once", action="store_true", help="Jalankan satu siklus lalu keluar")
    parser.add_argument("--lock-file", default=os.getenv("BIOSYNC_LOCK_FILE", "/tmp/biometric-sync.lock"))
    args = parser.parse_args()
    
    # sederhana PID lock berbasis exclusive create/open
    lock_path = args.lock_file
    # pastikan parent dir ada (untuk custom path selain /tmp)
    os.makedirs(os.path.dirname(lock_path) or ".", exist_ok=True)
    try:
        fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, str(os.getpid()).encode())
        os.close(fd)
    except FileExistsError:
        print(f"Another instance is running (lock: {lock_path}). Exit 0.")
        sys.exit(0)  # jangan dianggap error; biar timer nggak spam
    
    try:
        if args.once:
            main()
        else:
            infinite_loop()
    finally:
        try:
            os.remove(lock_path)
        except FileNotFoundError:
            pass
