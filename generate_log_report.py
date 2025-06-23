import os
import re
from collections import Counter
import json
from datetime import datetime


def log_parser(line: str)-> tuple:
    pattern = re.compile(
        r'(?P<ip>\d{1,3}(?:\.\d{1,3}){3})\s+'  # IP
        r'-\s+(?:[^\s"]+|-)\s+'  # ignore second field (user id or '-')
        r'\[(?P<date>[^\]]+)\]\s+'  # Date/time in []
        r'"(?P<method>[A-Z]+)\s+(?P<url>.+?)\s+HTTP/[\d.]+"\s+'  # Method, URL, HTTP/version
        r'(?P<duration>\d+)'  # Duration (число)
    )

    match = pattern.match(line)
    if not match:
        raise ValueError("Line does not match expected format")

    ip = match.group("ip")
    date = match.group("date")
    method = match.group("method")
    url = match.group("url")
    duration = int(match.group("duration"))
    return ip, date, method, url, duration

def process_log_file(file_path):
    """Функция для обработки одного .log файла"""
    filename = (os.path.basename(file_path))[:-4]
    total_requests = 0
    bad_lines = []
    ip_counter = Counter()
    method_counter = Counter()
    top_longest = []
    top_duration = []
    processed_result = dict()
    bad_lines_counter = 0

    with open(file_path, 'r') as f:
        for line in f:
            if line.startswith('#'):
                continue
            try:
                ip, date, method, url, duration = log_parser(line)
                total_requests += 1
                ip_counter[ip] += 1
                method_counter[method] += 1

                if len(top_longest) < 3:
                    top_longest.append({
                        "ip": ip,
                        "date": date,
                        "method": method,
                        "url": url,
                        "duration": duration
                    })
                    top_duration.append(duration)
                    continue

                min_duration = min(top_duration)
                if duration > min_duration:
                    min_index = top_duration.index(min_duration)
                    top_longest[min_index] = {
                        "ip": ip,
                        "date": date,
                        "method": method,
                        "url": url,
                        "duration": duration
                    }
                    top_duration[min_index] = duration

            except Exception:
                bad_lines.append(line.strip())
                bad_lines_counter += 1

    top_ips = sorted(ip_counter.items(), key=lambda x: x[1], reverse=True)
    processed_result["top_ips"] = top_ips[:3]
    processed_result["top_longest"] = sorted(top_longest, key=lambda x: x["duration"], reverse=True)
    processed_result["total_stat"] = method_counter
    processed_result["total_requests"] = total_requests
    file_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    if bad_lines:
        print(f"These {len(bad_lines)} bad line(s) were not parsed and not included to results")
        for bad_line in bad_lines:
            print(bad_line)

    with open(f"log_report_{filename}_{file_timestamp}.json", "w") as f:
        json.dump(processed_result, f, indent=4)
    print(f"Файл {filename} , был обработан и сохранен в папке ")
    print(json.dumps(processed_result, indent=4))
    print("BAD LINES HAVE BEEN FOUND:", bad_lines_counter)


def find_and_process_logs(directory):
    """Рекурсивно находит все .log файлы и обрабатывает их"""
    count = 0
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.log'):
                file_path = os.path.join(root, file)
                process_log_file(file_path)
                count += 1
    if count > 0:
        print(f"Был передан путь к каталогу и его oбработка завершена.\n"
              f"Обработано {count} файлов")
    else:
        print(f"В каталоге {directory} нет .log файлов")

def generate_log_report():
    path = input("Введите путь к .log файлу или директории с логами: ").strip()
    if not os.path.exists(path):
        return "Неверный путь"
    if os.path.isfile(path) and path.endswith('.log'):
        # Если передан один .log файл
        process_log_file(path)
        print("Был передан один .log файл и его oбработка завершена.\n")
    elif os.path.isdir(path):
        # Если передан каталог, ищем все .log файлы
        find_and_process_logs(path)
    else:
        return "Неподдерживаемый тип файла или путь"


if __name__ == "__main__":
    # "/Users/dzmitryhomza3ww939393mac/Downloads/access.log"
    generate_log_report()
