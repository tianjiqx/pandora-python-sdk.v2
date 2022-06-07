"""
Copyright 2020 Qiniu Cloud (qiniu.com)
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
 http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""


import logging
import sys
import traceback
import getopt
import pyarrow as pa
import pyarrow.flight

from .spl_packet_utils import *
from ..on_demand_action import OnDemandAction


class SplBaseCommand(OnDemandAction):
    def __init__(self):
        self.uri = 'http://127.0.0.1:9999'
        self.session = ''
        self.metainfo = None
        self.is_finish = False
        self.require_fields = ['*']
        self.export_fields = []
        self.lines = []
        self.spl_args = []
        # arrow flight 客户端
        self.flight_client = None
        # 输出 schema
        self.schema = None

    def on_request(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        opts = get_opts(argv)
        spl_version = 1
        if opts.get('-v'):
            spl_version = opts.get('-v')
        elif opts.get('--version'):
            spl_version = opts.get('version')

        if spl_version == 2:
            self.process_protocol_v2(opts)
        else:
            self.process_protocol(argv, input_stream, output_stream)

    def process_protocol(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        try:
            if argv is None:
                argv = sys.argv
            logging.debug('execute script command: {}'.format(argv))

            self.process_protocol_info(input_stream)
            self.init_env_by_getinfo()
            self.metainfo['require_fields'] = self.config_require_fields()
            self.metainfo['export_fields'] = self.config_export_fields()
            send_packet(output_stream, self.metainfo, [])
            self.after_getinfo()
            self.process_data(argv, input_stream, output_stream)
        except Exception as error:
            logging.exception(error)
            self.metainfo['error_message'] = "{}".format(error)
            self.metainfo['error_traceback'] = "{}".format(traceback.format_exc())
            send_packet(output_stream, self.metainfo, [])

    def process_data(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        """
        To be implemented
        """
        return

    def process_protocol_info(self, input_stream):
        meta_length, body_length = parse_head(input_stream)
        if meta_length <= 0:
            raise RuntimeError('GetInfo Protocol metaLength is invalid: {}'.format(meta_length))

        self.metainfo = parse_meta(input_stream, meta_length)

        # discard body in getinfo
        parse_body(input_stream, body_length)

    def process_protocol_execute(self, input_stream):
        meta_length, body_length = parse_head(input_stream)
        if meta_length <= 0:
            raise RuntimeError('Execute Protocol metaLength is invalid: {}'.format(meta_length))

        execute_meta = parse_meta(input_stream, meta_length)

        if execute_meta['action'] != "execute":
            raise RuntimeError('Execute Protocol action is invalid: {}'.format(execute_meta['action']))

        self.is_finish = execute_meta['finished']
        tmp = parse_body(input_stream, body_length)
        if len(tmp) > 0:
            self.lines.extend(tmp)
        return execute_meta

    def after_getinfo(self):
        return

    def init_env_by_getinfo(self):
        return

    def config_require_fields(self, require_fields=None):
        if require_fields is None:
            require_fields = []
        if require_fields is None or len(require_fields) == 0:
            require_fields = ['*']
        return require_fields

    def config_export_fields(self, export_fields=None):
        if export_fields is None:
            export_fields = []
        if export_fields is None or len(export_fields) == 0:
            export_fields = ['*']
        return export_fields

    def streaming_handle(self, lines):
        return lines, True

        # 向量化引擎处理， 基于flight
    def process_protocol_v2(self, opts=None):
        if opts is None:
            raise Exception('custom operator protocol v2 args is none！')

        # todo 处理新引擎协议
        # 解析参数 获取 uri, FlightDescriptor
        self.uri = try_get_opt(opts, '-u', '--uri')
        fd = try_get_opt(opts, '-fd', '--file_descriptor')
        action = try_get_opt(opts, '-a', '--action')
        descriptor = pa.flight.FlightDescriptor.for_path(fd)
        # 初始化 client
        self.init_arrow_flight_client()
        # 初始化 metainfo，通过 getFilightInfo 接口
        # metainfo = json.loads(meta_body)

        # TODO 解析参数类型，获取GetInfo，还是execute
        try:
            if action == 'GET_INFO':
                self.process_get_info(opts)
            elif action == "EXECUTION":
                # 初始化执行器等内容
                self.init_env_by_getinfo()
                self.process_data_v2(descriptor)
                self.after_getinfo()
            else:
                raise Exception("Unsuport action " + action)
        except Exception as error:
            logging.exception(error)
            #  检查是否有错误信息
            self.metainfo['error_message'] = "{}".format(error)
            self.metainfo['error_traceback'] = "{}".format(traceback.format_exc())
            # TODO 将错误信息作为结果返回
            # parser.send_packet(output_stream, self.metainfo, [])

    def init_arrow_flight_client(self):
        scheme = "grpc+tcp"
        uri = self.uri
        # todo tls 安全认证等配置
        self.flight_client = pa.flight.connect(f"{scheme}://{uri}")

    # 处理获取python 脚本定义信息，比如需要的字段，输出字段，新增字段？
    def process_get_info(self, opts=None):
        self.metainfo['require_fields'] = self.config_require_fields()
        self.metainfo['export_fields'] = self.config_export_fields()
        # 发送 meta info 回去, 设置需要字段
        # parser.send_packet(output_stream, self.metainfo, [])

    # 处理数据函数，获取输入，写输出结果
    def process_data_v2(self, fd):
        reader = self.flight_client.do_get(pa.flight.Ticket(bytes(fd.path, "uft-8")))
        # 设置 schema 根据脚本输出定义
        writer, _ = self.flight_client.do_put(fd, self.schema)
        while True:
            try:
                # RecordBatch  .to_pandas()
                batch = self.streaming_handle_v2(reader.read_next_batch())
                # 输出结果
                writer.write_batch(batch)
            except StopIteration:
                writer.close()
                self.finish()
                break

    # 待继承类覆盖，实现处理逻辑
    def streaming_handle_v2(self, batch):
        return batch, True

    # 结束操作
    def finish(self):
        return

def get_opts(argv=None):
    """
    解析参数
    :return: args dictionary
    """
    result = {}
    try:
        opts, args = getopt.getopt(argv, 'v:u:fd:', ['version=', 'uri=', 'file_descriptor='])
        result = dict(opts)
    except getopt.GetoptError:
        return result
    return result


def try_get_opt(opts, short_name, long_name):
    if opts.get(short_name):
        return opts.get(short_name)
    elif opts.get(short_name):
        return opts.get(short_name)
    else:
        raise Exception("Can't get " + short_name + " or " + long_name)