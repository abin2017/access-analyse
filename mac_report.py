import os
from datetime import datetime

import pycountry
from pyecharts.globals import ThemeType

from mac_logger import LogManager as logger
from mac_db import MacDatabase, Constable, ConsColumn, ConsKeyCode, ConsRGNCode
from config import *
import glob

import pandas as pd
from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.charts import Funnel
from pyecharts.render import engine as E
from jinja2 import Template


def get_db_files(folder_path):
    file_pattern = os.path.join(folder_path, '*.db')
    db_files = glob.glob(file_pattern)
    file_names = [os.path.basename(file) for file in db_files]
    return file_names


def get_country_name(country_code):
    try:
        country = pycountry.countries.get(alpha_2=country_code)
        if country is None:
            return country_code
        return country.name
    except LookupError:
        logger.warn('{} not found'.format(country_code))
        return country_code


def get_db_files(folder_path):
    file_pattern = os.path.join(folder_path, '*.db')
    db_files = glob.glob(file_pattern)
    file_names = [os.path.basename(file) for file in db_files]
    return file_names


def remove_extension(file_name):
    base_name = os.path.splitext(file_name)[0]
    return base_name


def _table_get_warn_time(d: MacDatabase):
    time = {'Prev Login': [], 'Next Login': [], 'Prev Time': [], 'Next Time': [], 'Interval': []}
    sql = 'SELECT {},{},{},{},{} FROM {}'.format(ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.START, ConsColumn.END,
                                                 ConsColumn.INTERVAL, Constable.WARN_TIME)
    result = d.exec_query(sql)
    for item in result:
        time['Prev Login'].append(item[0])
        time['Next Login'].append(item[1])
        dt = datetime.fromtimestamp(item[2])
        time['Prev Time'].append(dt.strftime('%Y-%m-%d %H:%M:%S'))
        dt = datetime.fromtimestamp(item[3])
        time['Next Time'].append(dt.strftime('%Y-%m-%d %H:%M:%S'))
        time['Interval'].append(item[4])
    df = pd.DataFrame(time)
    return df.to_html(classes='table', index=False)


def _table_get_warn_region(d: MacDatabase):
    region = {'Prev Login': [], 'Next Login': [], 'Interval': []}
    sql = 'SELECT {},{},{} FROM {}'.format(ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.WARN, Constable.WARN_REGION)
    result = d.exec_query(sql)
    for item in result:
        region['Prev Login'].append(item[0])
        region['Next Login'].append(item[1])
        if item[2] == ConsRGNCode.CHANGE_IN_5MIN:
            region['Interval'].append('In 5mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_10MIN:
            region['Interval'].append('In 10mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_20MIN:
            region['Interval'].append('In 20mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_30MIN:
            region['Interval'].append('In 30mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_60MIN:
            region['Interval'].append('In 1hour')
        elif item[2] == ConsRGNCode.CHANGE_IN_24HOUR:
            region['Interval'].append('In 24hour')
    df = pd.DataFrame(region)
    return df.to_html(classes='table', index=False)


def _table_get_warn_ip(d: MacDatabase):
    region = {'Prev Login': [], 'Next Login': [], 'Interval': []}
    sql = 'SELECT {},{},{} FROM {}'.format(ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.WARN, Constable.WARN_IP)
    result = d.exec_query(sql)
    for item in result:
        region['Prev Login'].append(item[0])
        region['Next Login'].append(item[1])
        if item[2] == ConsRGNCode.CHANGE_IN_5MIN:
            region['Interval'].append('In 5mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_10MIN:
            region['Interval'].append('In 10mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_20MIN:
            region['Interval'].append('In 20mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_30MIN:
            region['Interval'].append('In 30mins')
        elif item[2] == ConsRGNCode.CHANGE_IN_60MIN:
            region['Interval'].append('In 1hour')
        elif item[2] == ConsRGNCode.CHANGE_IN_24HOUR:
            region['Interval'].append('In 24hour')
    df = pd.DataFrame(region)
    return df.to_html(classes='table', index=False)


def _table_get_warn_option(d: MacDatabase):
    op = {'Mac': [], 'OptionId': []}
    sql = 'SELECT {} FROM {}'.format(ConsColumn.MAC, Constable.WARN_OPTION)
    result = d.exec_query(sql)
    for item in result:
        sql = 'SELECT {},{} FROM {} WHERE {}={}'.format(ConsColumn.MAC, ConsColumn.OPTION_ID, Constable.MAC,
                                                        ConsColumn.ID, item[0])
        tmp = d.exec_query(sql)
        if len(tmp):
            op['Mac'].append(tmp[0][0])
            op['OptionId'].append(tmp[0][1])
    df = pd.DataFrame(op)
    return df.to_html(classes='table', index=False)


def _table_get_warn_key(d: MacDatabase):
    region = {'Prev Login': [], 'Next Login': [], 'Reason': []}
    sql = 'SELECT {},{},{} FROM {}'.format(ConsColumn.LOGIN1, ConsColumn.LOGIN2, ConsColumn.WARN, Constable.WARN_KEY)
    result = d.exec_query(sql)
    for item in result:
        region['Prev Login'].append(item[0])
        region['Next Login'].append(item[1])
        if item[2] == ConsKeyCode.WARN_SAME_LAST:
            region['Reason'].append('Warn: take same key')
        elif item[2] == ConsKeyCode.WARN_REQ_KEY_EMPTY:
            region['Reason'].append('Warn: take empty key')
        elif item[2] == ConsKeyCode.ERROR_DIFFERENT:
            region['Reason'].append('Error: take wrong key')
        elif item[2] == ConsKeyCode.ERROR_NEW_KEY_EMPTY:
            region['Reason'].append('Error: assign empty key')
    df = pd.DataFrame(region)
    return df.to_html(classes='table', index=False)


def _table_get_warn_code(d: MacDatabase):
    op = {'Code': [], 'Mac1': [], 'Mac2': []}
    sql = 'SELECT {},{},{} FROM {}'.format(ConsColumn.CODE, ConsColumn.MAC1, ConsColumn.MAC2, Constable.WARN_CODE)
    result = d.exec_query(sql)
    for item in result:
        sql1 = 'SELECT {} FROM {} WHERE {}={}'.format(ConsColumn.CODE, Constable.CODE, ConsColumn.ID, item[0])
        sql2 = 'SELECT {},{} FROM {} WHERE {}={} OR {}={}'.format(ConsColumn.MAC, ConsColumn.OPTION_ID, Constable.MAC,
                                                                  ConsColumn.ID, item[1], ConsColumn.ID, item[2])
        tmp1 = d.exec_query(sql1)
        tmp2 = d.exec_query(sql2)
        if len(tmp1) and len(tmp2) > 1:
            op['Code'].append(tmp1[0][0])
            op['Mac1'].append('{} [OPTION] {}'.format(tmp2[0][0], tmp2[0][1]))
            op['Mac2'].append('{} [OPTION] {}'.format(tmp2[1][0], tmp2[1][1]))
    df = pd.DataFrame(op)
    return df.to_html(classes='table', index=False)


def _chart_create_funnel(d: MacDatabase):
    sql1 = 'SELECT {},{} FROM {}'.format(ConsColumn.ID, ConsColumn.REGION, Constable.REGION)
    result = d.exec_query(sql1)
    '''
    wf1 = Funnel()
    wf2 = Funnel()
    
    list_country = []
    list_box_counts = []
    list_req_counts = []
    for item in result:
        sql2 = "SELECT {} FROM {} WHERE {}= {}".format(ConsColumn.MAC, Constable.LOGIN, ConsColumn.REGION, item[0])
        result2 = d.exec_query(sql2)
        sets = set()
        for x in result2:
            sets.add(x[0])
        c = get_country_name(item[1])
        list_country.append(c)
        list_box_counts.append(len(sets))
        list_req_counts.append(len(result2))
    wf1.add('Mac counts', [list(z) for z in zip(list_country, list_box_counts)])
    wf2.add('Req counts', [list(z) for z in zip(list_country, list_req_counts)])

    
    return wf1, wf2
    '''
    # data_boxes = []
    # data_reqs = []

    list_country = []
    list_box_counts = []
    list_req_counts = []

    for item in result:
        sql2 = "SELECT {} FROM {} WHERE {}= {}".format(ConsColumn.MAC, Constable.LOGIN, ConsColumn.REGION, item[0])
        result2 = d.exec_query(sql2)
        sets = set()
        for x in result2:
            sets.add(x[0])
        c = get_country_name(item[1])
        # data_boxes.append((c, len(sets)))
        # data_reqs.append((c, len(result2)))
        list_country.append(c)
        list_box_counts.append(len(sets))
        list_req_counts.append(len(result2))
    '''
    funnel1 = (
        Funnel(init_opts=opts.InitOpts(theme=ThemeType.LIGHT))
            .add(
            series_name="Funnel Chart",
            data_pair=data_boxes,
            label_opts=opts.LabelOpts(position="inside"),
        ).set_global_opts(title_opts=opts.TitleOpts(title="Mac Counts"))
    )

    funnel2 = (
        Funnel(init_opts=opts.InitOpts(width="1800px", height="1200px", theme=ThemeType.LIGHT, is_horizontal_center=True))
            .add(
            series_name="Funnel Chart",
            data_pair=data_reqs,
            label_opts=opts.LabelOpts(position="inside"),
        ).set_global_opts(title_opts=opts.TitleOpts(title="Request Counts"))
    )
    '''
    funnel1 = (
        Bar(init_opts=opts.InitOpts(width="1800px", height="1000px", theme=ThemeType.LIGHT, is_horizontal_center=True))
        .add_xaxis(list_country)
        .add_yaxis("Mac", list_box_counts)
        .set_global_opts(title_opts=opts.TitleOpts(title="Mac Counts"))
    )

    funnel2 = (
        Bar(init_opts=opts.InitOpts(width="1800px", height="1000px", theme=ThemeType.LIGHT, is_horizontal_center=True))
        .add_xaxis(list_country)
        .add_yaxis("Request", list_req_counts)
        .set_global_opts(title_opts=opts.TitleOpts(title="Request Counts"))
    )

    return funnel1.render_embed(), funnel2.render_embed()


def parse_database_file(gl_data, file_name, db_dir):
    d = MacDatabase('{}/{}'.format(db_dir, file_name), False)
    d.connect()

    key_name = remove_extension(file_name)
    with open('template.html', 'r') as file:
        template_content = file.read()
    template = Template(template_content)

    detail = {}

    # mac count
    sql = 'SELECT COUNT(*) FROM {}'.format(Constable.MAC)
    result = d.exec_query(sql)
    detail['mac_count'] = 0
    if len(result) and len(result[0]):
        detail['mac_count'] = result[0][0]

    sql = 'SELECT COUNT(*) FROM {}'.format(Constable.REGION)
    result = d.exec_query(sql)
    detail['region_count'] = 0
    if len(result) and len(result[0]):
        detail['region_count'] = result[0][0]

    sql = 'SELECT COUNT(*) FROM {}'.format(Constable.LOGIN)
    result = d.exec_query(sql)
    detail['login_count'] = 0
    if len(result) and len(result[0]):
        detail['login_count'] = result[0][0]

    time_table_html = _table_get_warn_time(d)
    region_table_html = _table_get_warn_region(d)
    ip_table_html = _table_get_warn_ip(d)
    option_table_html = _table_get_warn_option(d)
    key_table_html = _table_get_warn_key(d)
    code_table_html = _table_get_warn_code(d)
    mic1_html, mic2_html = _chart_create_funnel(d)

    d.disconnect()
    gl_data[key_name] = detail

    rendered_html = template.render(title=key_name, cont_req_html=time_table_html, region_html=region_table_html,
                                    ip_html=ip_table_html, option_html=option_table_html, key_html=key_table_html,
                                    code_html=code_table_html, mic1_html=mic1_html, mic2_html=mic2_html)

    with open('{}/{}.html'.format(db_dir, key_name), 'w') as file:
        file.write(rendered_html)


def main():
    db_dir = get_value(CONF_DB_REPORT_DIR)
    db_list = get_db_files(db_dir)
    db_list.sort(reverse=False)
    logger.debug(db_list)

    gl_data = {}
    for item in db_list:
        parse_database_file(gl_data, item, db_dir)

    list_date = []
    list_mac = []
    list_region = []
    list_login = []
    for k, v in gl_data.items():
        list_date.append(k)
        if 'mac_count' in v:
            list_mac.append(v['mac_count'])
        else:
            list_mac.append(0)
        if 'region_count' in v:
            list_region.append(v['region_count'])
        else:
            list_region.append(0)
        if 'login_count' in v:
            list_login.append(v['login_count'])
        else:
            list_login.append(0)

    bar = Bar(init_opts=opts.InitOpts(width="1800px", height="1000px", theme=ThemeType.LIGHT, is_horizontal_center=True))
    bar.add_xaxis(list_date)
    bar.add_yaxis('MacCount', list_mac)
    bar.add_yaxis('CountryCount', list_region)
    bar.add_yaxis('VisitCount', list_login)
    bar.set_global_opts(title_opts=opts.TitleOpts(title='Statistics', subtitle='{}-{}'.format(
                                                                remove_extension(db_list[0]),
                                                                remove_extension(db_list[-1]))))
    bar.render(db_dir + '/Statistics.html')


if __name__ == '__main__':
    main()
