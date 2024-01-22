import aiohttp
import asyncio
import datetime
import time

import secure

from db_sql import insert_change_cards
from db_sql import insert_lessees
from db_sql import insert_lessors
from db_sql import insert_objects
from db_sql import insert_sign_cards
from db_sql import insert_stop_cards


async def change_proxy():
    num_procs = secure.num_proxs
    if secure.PROXY_ID < num_procs - 1:
        secure.PROXY_ID += 1
    else:
        secure.PROXY_ID = 0


def get_date_format(date_str, date_format):
    date = datetime.datetime.strptime(date_str, date_format)
    return date


async def get_card_data(card_dict):
    url = card_dict['guid']
    content = card_dict['content']

    done = 0
    '''
        CARDS
    '''
    type_card = card_dict['messageType']

    status = ''
    if type_card == 'FinancialLeaseContract' or type_card == 'FinancialLeaseContract2':
        status = 'sign'
    elif type_card == 'ChangeFinancialLeaseContract' or type_card == 'ChangeFinancialLeaseContract2':
        status = 'change'
    elif type_card == 'StopFinancialLeaseContract' or type_card == 'StopFinancialLeaseContract2':
        status = 'stop'

    real_id = card_dict['number']
    if 'contractInfo' in content:
        contract_info = content['contractInfo']
        dogovor = contract_info['number']
        dogovor_date = contract_info['date'].split('T')[0]
        dogovor_date = get_date_format(dogovor_date, '%Y-%m-%d')
    else:
        dogovor = content['contractNumber']
        dogovor_date = content['contractDate'].split('T')[0]
        dogovor_date = get_date_format(dogovor_date, '%Y-%m-%d')
    date_publish = card_dict['datePublish'].split('T')[0]
    date_publish = get_date_format(date_publish, '%Y-%m-%d')
    if 'financialLeasePeriod' in content:
        financial_lease_period = content['financialLeasePeriod']
        period, period_end, period_start = await get_period(financial_lease_period)
    else:
        period, period_end, period_start = await get_period(content)

    comments = ''
    if 'text' in content:
        comments = content['text']
        comments = comments.replace("'", '"')
        comments = comments.replace("\n", '')
    """
        CARDS_CHANGE
    """
    dogovor_main_real_id = ''
    dogovor_main_url = ''
    date_add = None
    if 'additionalInfo' in card_dict:
        add_info = card_dict['additionalInfo']['message']
        dogovor_main_real_id = add_info['number']
        dogovor_main_url = add_info['guid']
        date_add = time.strftime('%Y-%m-%d %H:%M:%S')
    """
        CARDS_STOP
    """
    reason_stop = ''
    dogovor_stop_date = None
    if 'stopReason' in content:
        reason_stop = content['stopReason']
        reason_stop = reason_stop.replace("'", '"')
        reason_stop = reason_stop.replace("\n", '')
        dogovor_stop_date = content['stopDate'].split('T')[0]
        dogovor_stop_date = get_date_format(dogovor_stop_date, '%Y-%m-%d')
    '''
        Лизингополучатель
    '''
    lessee_inn, lessee_name, lessee_ogrn = await get_bp(content, 'lessees')
    '''
        Лизингодатель
    '''
    lessor_inn, lessor_name, lessor_ogrn = await get_bp(content, 'lessors')
    '''
        CARD_OBJECTS
    '''
    object_name = ''
    object_class = ''
    object_description = ''
    object_guid = ''
    if status == 'change' and 'changedSubjects' in content and len(content['changedSubjects']) > 0:
        for el in content['changedSubjects']:
            subjects = el
            object_class = subjects['classifier']['code']
            if 'identifier' in subjects:
                object_name = subjects['identifier']
            if 'description' in subjects:
                object_description = subjects['description']
            if 'guid' in subjects:
                object_guid = subjects['guid']
            object_class, object_description, object_name = await fix_object_name(object_class, object_description,
                                                                                  object_name)
            object_total = f'{object_name} {object_class} {object_description}'
            if status == 'sign':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects')
            elif status == 'change':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects_change')
            elif status == 'stop':
                pass
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects_stop')
    elif 'subjects' in content and len(content['subjects']) > 0:
        for el in content['subjects']:
            subjects = el
            if 'identifier' in subjects:
                object_name = subjects['identifier']
            elif 'subjectId' in subjects:
                object_name = subjects['subjectId']
            if 'classifier' in subjects:
                object_class = subjects['classifier']['code']
            elif 'classifierCode' in subjects:
                object_class = subjects['classifierCode']
            if 'description' in subjects:
                object_description = subjects['description']
            if 'guid' in subjects:
                object_guid = subjects['guid']
            object_class, object_description, object_name = await fix_object_name(object_class, object_description,
                                                                                  object_name)
            object_total = f'{object_name} {object_class} {object_description}'
            if status == 'sign':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects')
            elif status == 'change':
                await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
                                     'card_objects_change')
            elif status == 'stop':
                pass
            # await insert_objects(url, object_guid, object_name, object_class, object_description, object_total,
            #                      'card_objects_stop')

    if status == 'sign':
        await insert_sign_cards(url, real_id, period, dogovor, dogovor_date, date_publish, type_card, period_start,
                                period_end, comments, done)
        await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees')
        await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors')
    elif status == 'change':
        await insert_change_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, dogovor_date,
                                  date_publish, type_card, period_start, period_end, date_add, comments, done)
        await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees_change')
        await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_change')
    elif status == 'stop':
        await insert_stop_cards(url, real_id, period, dogovor, dogovor_main_real_id, dogovor_main_url, reason_stop,
                                dogovor_date, dogovor_stop_date, date_publish, comments, type_card, done)
        await insert_lessees(url, lessee_name, lessee_inn, lessee_ogrn, 'card_lessees_stop')
        await insert_lessors(url, lessor_name, lessor_inn, lessor_ogrn, 'card_lessors_stop')


async def fix_object_name(object_class, object_description, object_name):
    object_name = object_name.replace("'", '"')
    object_name = object_name.replace('\n', '')
    object_class = object_class.replace('\n', '')
    object_class = object_class.replace("'", '"')
    object_description = object_description.replace('\n', '')
    object_description = object_description.replace("'", '"')
    return object_class, object_description, object_name


async def get_period(content):
    period = ''
    period_end = None
    period_start = None
    if 'startDate' in content:
        start_period = content['startDate'].split('T')[0]
        period_start = get_date_format(start_period, '%Y-%m-%d')
        end_period = content['endDate'].split('T')[0]
        period_end = get_date_format(end_period, '%Y-%m-%d')
        start_period = str(start_period).split('-')
        start_period = f'{start_period[2]}.{start_period[1]}.{start_period[0]}'
        end_period = str(end_period).split('-')
        end_period = f'{end_period[2]}.{end_period[1]}.{end_period[0]}'
        period = f'{start_period}-{end_period}'
    return period, period_end, period_start


async def get_bp(content, type_bp):
    name = ''
    inn = ''
    ogrn = ''
    fullname = ''
    bp_inn = 0
    bp_ogrn = 0
    if type_bp in content and len(content[type_bp]) > 0:
        name = type_bp
        if 'NonResidentCompany' in content[type_bp][0]['type']:
            # inn = 'regnum'
            bp_inn = '1111111111'
            fullname = 'name'
        else:
            if 'inn' in content[type_bp][0]:
                inn = 'inn'
            if 'ogrn' in content[type_bp][0]:
                ogrn = 'ogrn'
            elif 'ogrnip' in content[type_bp][0]:
                ogrn = 'ogrnip'
            if 'fullName' in content[type_bp][0]:
                fullname = 'fullName'
            elif 'fio' in content[type_bp][0]:
                fullname = 'fio'
    elif f'{type_bp}Companies' in content and len(content[f'{type_bp}Companies']) > 0:
        name = f'{type_bp}Companies'
        inn = 'inn'
        ogrn = 'ogrn'
        fullname = 'fullName'
    elif f'{type_bp}IndividualEntrepreneurs' in content and len(content[f'{type_bp}IndividualEntrepreneurs']) > 0:
        name = f'{type_bp}IndividualEntrepreneurs'
        inn = 'inn'
        ogrn = 'ogrnip'
        fullname = 'fio'
    elif f'{type_bp}Persons' in content and len(content[f'{type_bp}Persons']) > 0:
        name = f'{type_bp}Persons'
        inn = 'inn'
        fullname = 'fio'
    elif f'{type_bp}NonResidentCompanies' in content and len(content[f'{type_bp}NonResidentCompanies']) > 0:
        name = f'{type_bp}NonResidentCompanies'
        # inn = 'regnum'
        bp_inn = '1111111111'
        fullname = 'name'
    bp = content[name][0]
    bp_name = bp[fullname]
    if len(inn) > 1 and inn in bp:
        bp_inn = bp[inn]
    if len(ogrn) > 1 and ogrn in bp:
        bp_ogrn = bp[ogrn]
    bp_name = bp_name.replace("'", '"')
    bp_name = bp_name.replace('\n', '')
    return bp_inn, bp_name, bp_ogrn


async def find_cards(ids):
    aiohttp_client = aiohttp.ClientSession(connector=aiohttp.TCPConnector(ssl=False), trust_env=True)
    try:
        for num in ids:
            start_time = time.perf_counter()
            if secure.mode == 3:
                if len(str(num)) != 10 and len(str(num)) != 12:
                    continue
            await get_data(aiohttp_client, num)
            end_time = time.perf_counter()
            print(f"Elapsed time: {end_time - start_time:.2f} seconds")
    finally:
        await aiohttp_client.close()


async def get_data(aiohttp_client, num):
    url_find = 'https://fedresurs.ru/backend/encumbrances'
    if secure.mode == 3:
        delta_day = 90
        limit = 50
        start_date = datetime.date(2016, 1, 1)
        end_date = datetime.date.today()
        while start_date < end_date:
            date_end = start_date + datetime.timedelta(days=delta_day)
            split_day, shift, delta_day = await find_cards_by_inn(aiohttp_client, date_end, limit, num, start_date,
                                                                  url_find, delta_day)
            if not split_day and not shift:
                start_date = date_end + datetime.timedelta(days=1)
            if shift:
                delta_day = 90
                start_date = start_date + datetime.timedelta(days=1)
    elif secure.mode == 1 or secure.mode == 2:
        max_retries = 5
        retries = 0
        while retries < max_retries:
            try:
                headers_find = {
                    'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "YaBrowser";v="23"',
                    'Pragma': 'no-cache',
                    'DNT': '1',
                    'sec-ch-ua-mobile': '?0',
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                                  'Chrome/116.0.5845.1077 YaBrowser/23.9.1.1077 Yowser/2.5 Safari/537.36',
                    'Accept': 'application/json, text/plain, */*',
                    'Cache-Control': 'no-cache',
                    'Referer': f'https://fedresurs.ru/search/encumbrances?offset=0&limit=15&searchString={num}'
                               '&additionalSearchFnp=false&group=Leasing',
                    'sec-ch-ua-platform': '"Linux"',
                }
                params_find = {
                    'offset': f'0',
                    'limit': f'15',
                    'searchString': f'{num}',
                    'group': 'Leasing'
                }
                proxy_ip, proxy_port, proxy_user, proxy_pass = await secure.get_proxy_pref(3)
                proxy = f'htpp://{proxy_user}:{proxy_pass}@{proxy_ip}:{proxy_port}'
                r = await aiohttp_client.get(url=url_find, headers=headers_find, params=params_find, proxy=proxy)
                await change_proxy()
                find_dict = await r.json()
                found = find_dict['found']
                if found >= 1:
                    pages_data = find_dict['pageData']
                    # pages_len = len(pages_data)
                    for page in pages_data:
                        await get_page_data(aiohttp_client, num, page)
                else:
                    print(f'The card {num} does not exist')
                break
            except aiohttp.client_exceptions.ContentTypeError as ex:
                retries = await rep_by_ex(ex, max_retries, num, retries, 'Unexpected content type')
            except aiohttp.ClientError as e:
                retries = await rep_by_ex(e, max_retries, num, retries, 'aiohttp.ClientError')
            except asyncio.TimeoutError as ex:
                retries = await rep_by_ex(ex, max_retries, num, retries, 'asyncio.TimeoutError')
        if retries == max_retries:
            print(f'The card {num} does not exist')
            secure.log.write_log(f'The card {num} does not exist', '')


async def find_cards_by_inn(aiohttp_client, date_end, limit, num, start_date, url_find, delta_day):
    # ДЛЯ ОТКЛАДКИ
    # delta_day = 1
    # start_date = datetime.date(2020, 11, 25)
    # end_date = datetime.date(2020, 11, 25)
    # end_date = None
    max_retries = 5
    retries = 0
    offset = 0
    shift = False
    stop_count = False
    split = False
    split_hour = False
    split_minute = False
    hour = 0
    minute = 0
    start_date_str = ''
    end_date_str = ''
    hour_str = ''
    while True:
        # while retries < max_retries:
        try:
            if split_hour and not split_minute and not stop_count:
                shift = True
                if len(str(hour)) == 1:
                    hour_str = '0' + str(hour)
                else:
                    hour_str = str(hour)
                start_date_str = f'{start_date.strftime("%Y-%m-%d")}T{hour_str}:00:00.000'
                end_date_str = f'{start_date.strftime("%Y-%m-%d")}T{hour_str}:59:59.999'
                hour += 1
            elif split_hour and split_minute and not stop_count:
                if len(str(minute)) == 1:
                    minute_str = '0' + str(minute)
                else:
                    minute_str = str(minute)
                start_date_str = f'{start_date.strftime("%Y-%m-%d")}T{hour_str}:{minute_str}:00.000'
                end_date_str = f'{start_date.strftime("%Y-%m-%d")}T{hour_str}:{minute_str}:59.999'
                minute += 1
            elif not split_hour and not split_minute:
                start_date_str = f'{start_date.strftime("%Y-%m-%d")}T00:00:00.000'
                end_date_str = f'{date_end.strftime("%Y-%m-%d")}T23:59:59.999'
                # ДЛЯ ОТКЛАДКИ
                # end_date_str = f'{end_date.strftime("%Y-%m-%d")}T23:59:59.999'
            headers_find = {
                'sec-ch-ua': '"Chromium";v="116", "Not)A;Brand";v="24", "YaBrowser";v="23"',
                'Pragma': 'no-cache',
                'DNT': '1',
                'sec-ch-ua-mobile': '?0',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/116.0.5845.1077 YaBrowser/23.9.1.1077 Yowser/2.5 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Cache-Control': 'no-cache',
                'Referer': f'https://fedresurs.ru/search/encumbrances?offset={offset}&limit={limit}'
                           f'&searchString={num}&additionalSearchFnp=false&group=Leasing'
                           f'&publishDateStart={start_date_str}&publishDateEnd={end_date_str}',
                'sec-ch-ua-platform': '"Linux"',
            }
            params_find = {
                'offset': offset,
                'limit': limit,
                'searchString': num,
                'group': 'Leasing',
                'publishDateStart': start_date_str,
                'publishDateEnd': end_date_str,
                # ДЛЯ ОТКЛАДКИ
                # 'publishDateStart': '2020-11-25T00:00:00.000',
                # 'publishDateEnd': '2020-11-25T23:59:59.999',
            }
            print(f'START - {start_date_str}')
            print(f'END - {end_date_str}')
            proxy_ip, proxy_port, proxy_user, proxy_pass = await secure.get_proxy_pref(3)
            proxy = f'htpp://{proxy_user}:{proxy_pass}@{proxy_ip}:{proxy_port}'
            r = await aiohttp_client.get(url=url_find, headers=headers_find, params=params_find, proxy=proxy)
            await change_proxy()
            if r.status != 200:
                print(r.status)
            find_dict = await r.json()
            found = find_dict['found']
            pages_data = find_dict['pageData']
            pages_len = len(pages_data)
            if found != 0 and found <= 500 and pages_len > 0:
                stop_count = True
                for page in pages_data:
                    await get_page_data(aiohttp_client, num, page)
                offset += pages_len
            elif found > 500:
                if delta_day == 1:
                    if not split_hour:
                        split_hour = True
                    elif not split_minute and split_hour:
                        split_minute = True
                else:
                    delta_day /= 2
                    split = True
                if not split_hour and not split_minute:
                    return split, shift, int(delta_day)
            else:
                if found == 0:
                    print(f'The cards with inn {num} does not found in range publishDateStart {start_date_str} - '
                          f'publishDateEnd {end_date_str}')
                elif found > 0:
                    if not stop_count:
                        delta_day = 90
                    else:
                        stop_count = False
                        offset = 0
                if minute == 60:
                    split_minute = False
                    minute = 0
                if hour == 24:
                    split_hour = False
                    hour = 0
                if not split_hour:
                    break
            # ДЛЯ ОШИБОК
            # break
        except aiohttp.client_exceptions.ContentTypeError as ex:
            retries = await rep_by_ex(ex, max_retries, num, retries, 'Unexpected content type')
        except aiohttp.ClientError as e:
            retries = await rep_by_ex(e, max_retries, num, retries, 'aiohttp.ClientError')
        except asyncio.TimeoutError as ex:
            retries = await rep_by_ex(ex, max_retries, num, retries, 'asyncio.TimeoutError')
    if retries == max_retries:
        print(f'The card {num} does not exist')
        secure.log.write_log(f'The card {num} does not exist', '')
    return split, shift, int(delta_day)


async def get_page_data(aiohttp_client, num, page):
    max_retries = 5
    retries = 0
    while retries < max_retries:
        try:
            if secure.mode == 3:
                num = page['number']
            print(f'The card {num} has been found')
            # page_data = find_dict['pageData'][0]
            # guid = page_data['guid']
            guid = page['guid']
            headers_card = {
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'Pragma': 'no-cache',
                'sec-ch-ua-mobile': '?0',
                'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) '
                              'Chrome/120.0.0.0 Safari/537.36',
                'Accept': 'application/json, text/plain, */*',
                'Cache-Control': 'no-cache',
                'Referer': f'https://fedresurs.ru/sfactmessage/{guid}',
                'sec-ch-ua-platform': '"Linux"',
            }
            url_card = f'https://fedresurs.ru/backend/sfactmessages/{guid}'
            proxy_ip, proxy_port, proxy_user, proxy_pass = await secure.get_proxy_pref(3)
            proxy = f'htpp://{proxy_user}:{proxy_pass}@{proxy_ip}:{proxy_port}'
            # print(proxy)
            r2 = await aiohttp_client.get(url=url_card, headers=headers_card, proxy=proxy)
            await change_proxy()
            card_dict = await r2.json()
            await get_card_data(card_dict)
            break
        except aiohttp.client_exceptions.ContentTypeError as ex:
            if secure.mode == 3:
                num = page['number']
            retries = await rep_by_ex(ex, max_retries, num, retries, 'Unexpected content type')
        except aiohttp.ClientError as e:
            if secure.mode == 3:
                num = page['number']
            retries = await rep_by_ex(e, max_retries, num, retries, 'aiohttp.ClientError')
        except asyncio.TimeoutError as ex:
            if secure.mode == 3:
                num = page['number']
            retries = await rep_by_ex(ex, max_retries, num, retries, 'asyncio.TimeoutError')
    if retries == max_retries:
        if secure.mode == 3:
            num = page['number']
        print(f'The card {num} does not exist')
        secure.log.write_log(f'The card {num} does not exist', '')


async def rep_by_ex(ex, max_retries, num, retries, reason):
    print(f"Repeat data scrap {num}")
    secure.log.write_log(f"Error: {reason} for num={num}", ex)
    secure.log.write_log(f"Repeat data scrap {num}", '')
    await change_proxy()
    retries += 1
    print(f"Retry {retries}/{max_retries}")
    secure.log.write_log(f"Retry {retries}/{max_retries}", '')
    return retries
