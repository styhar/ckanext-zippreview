import ckan.plugins as plugins
import ckan.plugins.toolkit as toolkit
import os
import zipfile
from ckan.lib import uploader, formatters
import requests, cStringIO
from collections import OrderedDict
import urllib2, struct, sys

def getZipListFromURL(url):


    def getList(start):
        fp = cStringIO.StringIO(requests.get(url, headers={'Range': 'bytes={}-{}'.format(start,end)}).content)
        zf = zipfile.ZipFile(fp)
        return zf.filelist

    def getListAdvanced(url):
        # https://superuser.com/questions/981301/is-there-a-way-to-download-parts-of-the-content-of-a-zip-file
        def open_remote_zip(url, offset=0):
            return urllib2.urlopen(urllib2.Request(url, headers={'Range': 'bytes={}-'.format(offset)}))

        offset = 0
        fp = open_remote_zip(url)
        header = fp.read(30)
        list = []
        while header[:4] == 'PK\x03\x04':
            compressed_len, uncompressed_len = struct.unpack('<II', header[18:26])
            filename_len, extra_len = struct.unpack('<HH', header[26:30])
            header_len = 30 + filename_len + extra_len
            total_len = header_len + compressed_len
            filename = fp.read(filename_len)
            #print('{}\n offset: {}\n length: {}\n  header: {}\n  payload: {}\n uncompressed length: {}'.format(filename, offset, total_len, header_len, compressed_len, uncompressed_len))
            zi = zipfile.ZipInfo(filename)
            zi.file_size = uncompressed_len
            list.append(zi)
            fp.close()

            offset += total_len
            fp = open_remote_zip(url, offset)
            header = fp.read(30)

        fp.close()
        return list
    try :
        head = requests.head(url)
        if 'content-length' in head.headers:
            end = int(head.headers['content-length'])

        if 'content-range' in head.headers:
            end = int(head.headers['content-range'].split("/")[1])
        return getList(end - 65536)
    except Exception, e:
        pass
    try:
        return getListAdvanced(url)
    except Exception, e:
        return None

def zip_list(rsc):
    if rsc.get('url_type') == 'upload':
        upload = uploader.ResourceUpload(rsc)
        value = None
        try:
            zf = zipfile.ZipFile(upload.get_path(rsc['id']),'r')
            value = zf.filelist
        except Exception:
            # Sometimes values that can't be converted to ints can sneak
            # into the db. In this case, just leave them as they are.
            pass
        return value
    else:

        return getZipListFromURL(rsc.get('url'))
    return None


def zip_tree(rsc):
    list = zip_list(rsc)

    def get_icon(item):
        extension = item.split('.')[-1].lower()
        if extension in ['xml', 'txt', 'json']:
            return "file-text"
        if extension in ['csv', 'xls']:
            return "bar-chart"
        if extension in ['shp', 'geojson', 'kml', 'kmz']:
            return "globe"
        return "file"

    if not list:
        return None

    tree = OrderedDict()
    for compressed_file in list:
        if "/" not in compressed_file.filename:
            tree[compressed_file.filename] = {"title": compressed_file.filename,
                                              "file_size": (formatters.localised_filesize(compressed_file.file_size)),
                                              "children": [],
                                              "icon": get_icon(compressed_file.filename)}
        else:
            parts = compressed_file.filename.split("/")
            if parts[-1] != "":
                child = {"title": parts.pop(),
                         "file_size": (formatters.localised_filesize(compressed_file.file_size)),
                         "children": [], "icon": get_icon(compressed_file.filename)}
                parent = '/'.join(parts)
                if parent not in tree:
                    tree[parent] = {"title": parent, "children": [], "icon": 'folder-open'}
                tree[parent]['children'].append(child)

    return tree.values()


class ZipPreviewPlugin (plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IResourceView, inherit=True)
    plugins.implements(plugins.ITemplateHelpers, inherit=False)

    # IConfigurer

    def update_config(self, config_):
        toolkit.add_template_directory(config_, 'templates')
        toolkit.add_public_directory(config_, 'public')
        toolkit.add_resource('fanstatic', 'zippreview')

    def get_helpers(self):
        return {'zip_tree': zip_tree}

    def info(self):
        return {'name': 'zip_view', 'title': 'ZIP Viewer', 'default_title': 'ZIP Viewer',
                'icon': 'folder-open'}

    def can_view(self, data_dict):
        resource = data_dict['resource']
        format_lower = resource['format'].lower()
        if (format_lower == ''):
            format_lower = os.path.splitext(resource['url'])[1][1:].lower()
        # print format_lower
        if format_lower in ['zip', 'application/zip', 'application/x-zip-compressed']:
            return True
        return False

    def view_template(self, context, data_dict):
        return 'zip.html'
