import time
import threading
import random

import click
from influxdb.client import InfluxDBClient


def generate_fake_data(points):  # type: (int) -> list(dict)
    return [
        {
            "measurement": "cpu_load_short",
            "tags": {
                "host": "test",
            },
            "time": int(time.time() * 1000000),
            "fields": {
                "value": float(random.randint(0, 100)) / 100
            }
        }
        for _ in range(points)
    ]


def do_influx_request(n_points, host, database):  # type: (int, str, str) -> None
    conn = InfluxDBClient(host=host)
    conn.switch_database('p2')
    points = generate_fake_data(n_points)
    conn.write_points(points, time_precision='u', database=database)


def thread_run_request(points, seconds, host, database):  # type: (int, int, str, str) -> None
    end_time = time.time() + seconds
    request_count = 0  # type: int
    while time.time() < end_time:
        # do request
        do_influx_request(points, host, database)
        request_count += 1
    print('request %d in %d seconds with %d points!' % (request_count, seconds, points))


@click.command()
@click.option('--points', default=100, help='每个请求带多少个数据点')
@click.option('--seconds', default=10, help='持续运行多少时间')
@click.option('--threads', default=4, help='使用多少个线程')
@click.option('--host', default='172.20.1.30', help='InfluxDB-relay 的域名或者IP地址')
@click.option('--database', default='test', help='写入到哪个数据库')
def main(points, seconds, threads, host, database):  # type: (int, int, int, str, str) -> None
    """
    简单的向 InfluxDB-relay 写入数据的工具
    """
    running_threads = []  # type: list(threading.Thread)

    for i in range(threads):
        t = threading.Thread(target=lambda: thread_run_request(points, seconds, host, database))
        t.start()
        running_threads.append(t)

    for t in running_threads:
        t.join()


if __name__ == '__main__':
    main()
