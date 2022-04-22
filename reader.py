

import struct
from imagecodecs import lzw_decode
import math
from enum import Enum
from time import time
from concurrent.futures import ThreadPoolExecutor
from sys import argv
import json

def get_row_col(index, width):
    row = math.floor(index / width)
    col = index % width
    return [row, col]

def get_headers(f, tags, num_entries, byte_order):
    header_items = {}
    i = 0
    while len(tags) > 0 and i < num_entries:
        field_tag = f.read(2)
        field_tag = int.from_bytes(field_tag, byte_order)
        if field_tag in tags:
            tags.remove(field_tag)
            #all of the tags required should store an offset (data not less than or equal to 4 bytes)
            #only need to get one item, know how many down so no need to get field length or any of that (assuming hardcoded resolution is correct)
            #get data offset
            f.seek(6, 1)
            data_offset = f.read(4)
            data_offset = int.from_bytes(data_offset, byte_order)
            header_items[field_tag] = data_offset
        else:
            #skip to the next header field
            f.seek(10, 1)
        i += 1
    return header_items


def get_index_value(file, index):
    with open(file, "rb") as f:
        byte_order = f.read(2).decode("utf-8")
        if byte_order == "II":
            byte_order = "little"
        elif byte_order == "MM":
            byte_order = "big"
        else:
            raise Exception("Invalid byte order")
        tiff_id = f.read(2)
        tiff_id = int.from_bytes(tiff_id, byte_order)
        if tiff_id != 42:
            raise Exception("Not a tiff")
        first_ifd_offset = f.read(4)
        first_ifd_offset = int.from_bytes(first_ifd_offset, byte_order)
        f.seek(first_ifd_offset)

        num_entries = f.read(2)
        num_entries = int.from_bytes(num_entries, byte_order)

        #273: strip offsets; 279: strip byte counts
        tags = [273, 279]
        header_data_offsets = get_headers(f, tags, num_entries, byte_order)

        #hardcode for efficiency, tag 256 if need to get from header
        row, col = get_row_col(index, 2288)

        #add the row times 4 bytes per row to offset
        strip_offset_offset = header_data_offsets[273] + row * 4
        strip_byte_count_offset = header_data_offsets[279] + row * 4
        #jump to strip offset
        f.seek(strip_offset_offset)
        #read strip offset
        strip_offset = f.read(4)
        strip_offset = int.from_bytes(strip_offset, byte_order)
        #jump to strip size
        f.seek(strip_byte_count_offset)
        #read strip size
        strip_size = f.read(4)
        strip_size = int.from_bytes(strip_size, byte_order)
        #go to strip
        f.seek(strip_offset)
        #read strip
        strip = f.read(strip_size)
        #decompress_strip (is there a way to decompress part of the strip? would have to make own decompression)
        strip = lzw_decode(strip)
        unpack_dir = "<f" if byte_order == "little" else "f>"
        #value starts at 4 time col
        val_start = col * 4
        #resolve value from strip to 32 bit float
        val = struct.unpack(unpack_dir, strip[val_start:val_start + 4])[0]
        return val

start = time()
vals = [None] * (len(argv) - 2)
with ThreadPoolExecutor() as executor:
    index = int(argv[1])
    for i in range(19):
        for i in range(2, len(argv)):
            file = argv[i]
            f = executor.submit(get_index_value, file, index)
            vals[i - 2] = f.result()
print(time() - start)

print(vals)

# val = get_index_value("rainfall_new_month_statewide_data_map_2022_01.tif", 43664)








