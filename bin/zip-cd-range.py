import struct
import requests
import cStringIO
from zipfile import ZipInfo
import zipfile
import urllib2, struct, sys
def _GetContents2(url):
    head = requests.head(url)
    end = int(head.headers['content-length'])

    def getList(start):
        fp = cStringIO.StringIO(requests.get(url, headers={'Range': 'bytes={}-{}'.format(start,end)}).content)
        zf = zipfile.ZipFile(fp)
        return zf.namelist()
    try :
        return getList(end - 65536)
    except:
        pass
    try:
        return getList(end - 2097152)
    except:
        return None

def _GetContents(url):
    # Here are some struct module formats for reading headers
    structEndArchive = "<4s4H2lH"     # 9 items, end of archive, 22 bytes
    stringEndArchive = "PK\005\006"   # magic number for end of archive record
    structCentralDir = "<4s4B4H3l5H2l"# 19 items, central directory, 46 bytes
    stringCentralDir = "PK\001\002"   # magic number for central directory
    structFileHeader = "<4s2B4H3l2H"  # 12 items, file header record, 30 bytes
    stringFileHeader = "PK\003\004"   # magic number for file header
    "Read in the table of contents for the zip file"
    head = requests.head(url)
    end = int(head.headers['content-length'])
    start = end - 64000

    fp = cStringIO.StringIO(requests.get(url, headers={'Range': 'bytes={}-{}'.format(start,end)}).content) #open('/var/www/SH54.zip.tail','r')
    NameToInfo = {}    # Find file info given name
    filelist = []
    fp.seek(-22, 2)		# Start of end-of-archive record
    filesize = fp.tell() + 22	# Get file size
    endrec = fp.read(22)	# Archive must not end with a comment!
    if endrec[0:4] != stringEndArchive or endrec[-2:] != "\000\000":
        return
    endrec = struct.unpack(structEndArchive, endrec)
    #print endrec
    size_cd = endrec[5]		# bytes in central directory
    offset_cd = endrec[6]	# offset of central directory
    x = filesize - 22 - size_cd
    # "concat" is zero, unless zip was concatenated to another file
    concat = x - offset_cd
    #print "given, inferred, offset", offset_cd, x, concat
    # self.start_dir:  Position of start of central directory
    start_dir = offset_cd + concat
    fp.seek(start_dir, 0)
    total = 0
    while total < size_cd:
        centdir = fp.read(46)
        total = total + 46
        if centdir[0:4] != stringCentralDir:
            return
        centdir = struct.unpack(structCentralDir, centdir)
        #print centdir
        filename = fp.read(centdir[12])
        # Create ZipInfo instance to store file information
        x = ZipInfo(filename)
        x.extra = fp.read(centdir[13])
        x.comment = fp.read(centdir[14])
        total = total + centdir[12] + centdir[13] + centdir[14]
        x.header_offset = centdir[18] + concat
        #x.file_offset = x.header_offset + 30 + centdir[12] + centdir[13]
        (x.create_version, x.create_system, x.extract_version, x.reserved,
         x.flag_bits, x.compress_type, t, d,
         x.CRC, x.compress_size, x.file_size) = centdir[1:12]
        x.volume, x.internal_attr, x.external_attr = centdir[15:18]
        # Convert date/time code to (year, month, day, hour, min, sec)
        x.date_time = ( (d>>9)+1980, (d>>5)&0xF, d&0x1F,
                        t>>11, (t>>5)&0x3F, t&0x1F * 2 )
        filelist.append(x)
        NameToInfo[x.filename] = x
        #print "total", total
    return [x.filename for x in filelist]

def _GetContents3(url):
    def open_remote_zip(url, offset=0):
        return urllib2.urlopen(urllib2.Request(url, headers={'Range': 'bytes={}-'.format(offset)}))

    offset = 0
    zipfile = open_remote_zip(url)
    header = zipfile.read(30)

    while header[:4] == 'PK\x03\x04':
        compressed_len, uncompressed_len = struct.unpack('<II', header[18:26])
        filename_len, extra_len = struct.unpack('<HH', header[26:30])
        header_len = 30 + filename_len + extra_len
        total_len = header_len + compressed_len

        print('{}\n offset: {}\n length: {}\n  header: {}\n  payload: {}\n uncompressed length: {}'.format(zipfile.read(filename_len), offset, total_len, header_len, compressed_len, uncompressed_len))
        zipfile.close()

        offset += total_len
        zipfile = open_remote_zip(url, offset)
        header = zipfile.read(30)

    zipfile.close()

print _GetContents("https://data.gov.au/dataset/54f906a3-2c6c-4143-bcb4-27d542429939/resource/6f168155-04d5-42af-a584-fa7f412d745f/download/NativeTitleNatshp.zip")
#print _GetContents2("http://www.rm.cloudns.org/~xonotic/xonotic-0.8.1.zip")
#print _GetContents3("https://data.gov.au/dataset/54f906a3-2c6c-4143-bcb4-27d542429939/resource/6f168155-04d5-42af-a584-fa7f412d745f/download/NativeTitleNatshp.zip")
