import io
import csv
import sys
import requests
import xml.etree.ElementTree as ET
import mysql.connector

from random import randint
from hashlib import md5


def get_link_to_download(from_date, to_date, random_num):
    """returns list of links to download products,
       function takes date range to determine products to download """

    url = 'https://scihub.copernicus.eu/dhus/search?start={0}&rows=1&q=beginposition:[{1}%20TO%20{2}]'
    formated_url = url.format(random_num, from_date, to_date)
    xml_data = requests.get(formated_url, auth=('ejsmont', 'kr0l0lch')).content

    root = ET.fromstring(xml_data)
    for child in root.iter('{http://www.w3.org/2005/Atom}link'):
        if len(child.attrib['href']) == 98:
            link_to_download = child.attrib['href']
    return link_to_download


def check_md5(product, link_to_meta):
    """computes md5 sum and returns True if md5 is correct"""

    response = requests.get(link_to_meta, auth=('ejsmont', 'kr0l0lch'))
    meta_content = response.content
    root = ET.fromstring(meta_content)
    for child in root.iter():
        if child.tag == '{http://schemas.microsoft.com/ado/2007/08/dataservices}Value':
            checksum_received = str(child.text).lower()

    hash_object = md5(product)
    checksum_computet = hash_object.hexdigest()

    if checksum_computet == checksum_received:
        return True


connector_string = {
    'host': 'localhost',
    'port': 3306,
    'user': 'esa_user',
    'passwd': 'esa_password',
    'database': 'ESA_DB'
}
NUM_OF_DOWNLOADS = 3


def main(date_from, date_to):
    for count in range(0, NUM_OF_DOWNLOADS):
        random_num = randint(0, 10)
        link_to_download = get_link_to_download(date_from, date_to, random_num)
        link_to_meta = link_to_download[:-7]
        response = requests.get(link_to_download, auth=('ejsmont', 'kr0l0lch'))
        content = response.content

        # md5 check
        if not check_md5(content, link_to_meta):
            print("Error, download run incorrectly, md5 is incorrect")
            exit(1)

        # db initialization
        esa_db = mysql.connector.connect(**connector_string)
        cursor = esa_db.cursor()

        # file save
        with open('product{0}.zip'.format(count), 'wb') as zip_file:
            import zipfile
            zipfile_handler = zipfile.ZipFile(io.BytesIO(content))
            for file_info in zipfile_handler.infolist():
                with zipfile_handler.open(file_info) as a_file:
                    hash_object = md5(a_file.read())
                    hash_value = hash_object.hexdigest()
                    sql = 'INSERT INTO esa_content (filename, path, md5_checksum) VALUES (%s, %s, %s)'
                    full_path = file_info.filename.split('/')
                    filename = full_path[-1]
                    path = ''.join(full_path[0:-1])
                    values = (filename, path, hash_value)
                    cursor.execute(sql, values)
                    esa_db.commit()
            zip_file.write(content)

        # sql query
        extraction_sql = 'select filename, path, md5_checksum from esa_content';
        cursor.execute(extraction_sql)
        all_rows = cursor.fetchall()

        # create csv
        with open('esa_data.csv', 'w', newline='') as esa_data_file:
            csv_writer = csv.writer(esa_data_file)
            for row in all_rows:
                csv_writer.writerow(row)

        # save datarange log
        with open('timerange.txt', 'w') as log_file:
            log_file.write('{0}-{1}'.format(date_from, date_to))


if __name__ == '__main__':
    assert len(sys.argv) == 3
    main(*(sys.argv[1:]))
