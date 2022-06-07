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

import sys
import pyarrow as pa
import pyarrow.flight

from .spl_packet_utils import *
from .spl_base_command import SplBaseCommand


class SplGeneratingCommand(SplBaseCommand):
    def generate(self):
        lines = []
        return lines

    def process_data(self, argv=None, input_stream=sys.stdin.buffer, output_stream=sys.__stdout__.buffer):
        while True:
            execute_meta = self.process_protocol_execute(input_stream)
            resp = self.generate()
            send_packet(output_stream, execute_meta, resp)
            self.lines = []
            if self.is_finish:
                break

    # 向量化引擎
    def process_data_v2(self, fd):
        # client 获取 输入流
        # reader = self.flight_client.do_get(pa.flight.Ticket(bytes(fd.path, "uft-8")))
        writer, _ = self.flight_client.do_put(fd, self.schema)
        while True:
            # RecordBatch  .to_pandas()
            batch = self.generate_v2()
            # 输出结果
            if batch:
                writer.write_batch(batch)
            if self.is_finish:
                writer.close()
                self.finish()
                break

    def generate_v2(self):
        return None
