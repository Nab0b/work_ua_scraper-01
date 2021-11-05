import sqlite3
import os.path
from sqlite3.dbapi2 import Error
import datetime
import time
import lxml.html as x
import unicodedata
import re
from torrequest import TorRequest


CV_XPATHS = ( 
    ("name", r"//h1/text()"),
    ("age", r"//dd[1]/text()"),
    ("title", r"//div[contains(@class,'col')]/h2/text()"),
    ("is_remote", r"//div[contains(@class,'col')]/p[@class='text-muted']/text()"),
    ("salary", r"//div[contains(@class,'col')]/h2/span[@class='normal-weight text-muted-print']/text()"),
    )
JOB_XPATHS = (
    ("company name",r"//p[@class='text-indent text-muted add-top-sm'][2]/a/b/text()"),
    ("company size",r"//p[@class='text-indent text-muted add-top-sm'][2]/span[@class='add-top-xs']/span[@class='nowrap']/text()"),
    ("title",r"//h1[@id='h1-name']/text()"),
    ("salary",r"//p[@class='text-indent text-muted add-top-sm'][1]/b[@class='text-black']/text()"),
    ("is_remote",r"//p[@class='text-indent add-top-sm'][2]/text()[2]"),
    ("required_xp",r"//p[@class='text-indent add-top-sm'][2]/text()[2]"), #можно оптимизировать в get_xpath() - 3 одинаковые проверки в 1
    ("required_education",r"//p[@class='text-indent add-top-sm']/text()"))
LAST_LIST_PAGE_XPATH = (
    r"/html/body/section[@id='center']/div[@id='pjax']/div[@class='row']/div[@class='col-md-8 col-left']/div[@id='pjax-job-list']/nav/ul[@class='pagination hidden-xs']/li[@class='no-style disabled']/span",
    r"/html/body/section[@id='center']/div[@id='pjax']/div[@class='container']/div[@class='row']/div[@class='col-md-8 col-right']/div[@id='pjax-resume-list']/nav/ul[@class='pagination hidden-xs']/li[@class='no-style disabled']/span")
CV_DATA_PAGE_LINK_FROM_LIST_XPATH = "//div[contains(@class,'col')]/div[@id='pjax-resume-list']/div[contains(@class,'card')][{}]/h2/a"
JOBS_DATA_PAGE_LINK_FROM_LIST_XPATH = "//div[contains(@class,'col-md')]/div[@id='pjax-job-list']/div[contains(@class,'job-link')][{}]/h2/a"

DB_PATH = r"C:\Users\Pure Rage\Documents\Python scripts\work_ua_scraper\db.db"

START_CV_LIST_PAGE_URL = "https://www.work.ua/resumes-kyiv/?page={page_n}"
START_JOBS_LIST_PAGE_URL = "https://www.work.ua/jobs-kyiv/?page={page_n}"
CREATE_JOB_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS JOBS 
            (date DATE,
            company_name,
            company_size,
            title,
            salary,
            is_remote INT,
            required_xp,
            required_education,
            url);"""
CREATE_CV_TABLE_QUERY = """CREATE TABLE IF NOT EXISTS CVs 
            (date date,
            name text,
            age int,
            title text,
            is_remote int,
            salary int,
            xp int,
            education int,
            url);"""
CV_INSERT_QUERY = "INSERT INTO CVs VALUES (?,?,?,?,?,?,?,?,?)"
JOBS_INSERT_QUERY = "INSERT INTO JOBS VALUES (?,?,?,?,?,?,?,?,?)"

tor_request = TorRequest(proxy_port=9050, ctrl_port=9051, password=None)
tor_request.session.proxies.update({'https': 'socks5h://localhost:9050',})
ip_change_counter = 0
ip_change_limit  = 100


def get_page(url): 
    global ip_change_counter
    global tor_request
    # page = x.fromstring(requests.get(url,proxies={"http://":proxy_ip}).content)
    try:
        page = x.fromstring(tor_request.get(url).content)
    except ConnectionError as er:
        print(er)
        time.sleep(1)
    ip_change_counter += 1
    if ip_change_counter == ip_change_limit:
        tor_request.reset_identity_async()
        ip_change_counter = 0
    return page
        
def concat(list1,*args):
    for listn in args:
        for i in listn:
            list1.append(i)
    return list1

def remove_double_whitespaces(string): 
    return re.sub(r"\s+|\n"," ",string)

def create_db():
    if not os.path.isfile(DB_PATH):
        conn = new_connection(DB_PATH)
        cur = conn.cursor()
        cur.execute(CREATE_JOB_TABLE_QUERY)
        cur.execute(CREATE_CV_TABLE_QUERY)
        conn.commit()
        conn.close()
    # create 2 tables : "jobs" and "cvs" with columns according to
    # return none

def new_connection(DB_PATH):
    conn = None
    try:
        conn = sqlite3.connect(DB_PATH)
        return conn
    except sqlite3.Error as e:
        print(e)
    return conn

def check_list_page_if_last(url):
    try:
        list_page = get_page(url)
    except ConnectionError:
        time.sleep(20)
        list_page = get_page(url)
    if list_page.xpath("//div[@class='card']"): #non active element
        return False
    return True
    # return true if last false if not 
    
def get_data_page_urls_from_list_page(list_page_url, cv_or_job):
    # try:
    list_page = get_page(list_page_url)
    # except requests.exceptions.ProxyError as e:
    #     print("proxy error, generating new proxy list")
    #     generate_proxylist()
    data_pages_urls = []
    for i in range(1,17):
        if cv_or_job == "cv":
            xpaths = CV_DATA_PAGE_LINK_FROM_LIST_XPATH
        else:
            xpaths = JOBS_DATA_PAGE_LINK_FROM_LIST_XPATH
        try:
            url = list_page.xpath(xpaths.format(str(i)))[0].get('href')
            url = "https://www.work.ua" + url
            data_pages_urls.append(url)
        except IndexError:
            continue
    print(data_pages_urls)
    return data_pages_urls
    # return list of job\cvs page urls from single list-page (list_page_url) via xpath (different xpath for cv and job)

def get_xpath_data(page, cv_or_job):
    result_list = []
    if cv_or_job == "cv":
        xpaths = CV_XPATHS
    else:
        xpaths = JOB_XPATHS
    for p in xpaths:
        try:
            result_list.append([p[0],unicodedata.normalize('NFKC', page.xpath(p[1])[0].strip())])
        except IndexError:
            result_list.append([p[0], ""])
    if cv_or_job == "job":
        if re.findall(r"дистанційна",result_list[4][1]):
            result_list[4][1] = 1
        else:
            result_list[4][1] = 0
        if result_list[5][1]:
            if re.findall(r"(\d+(?=(\s+)?рік)|\d+(?=(\s+)?роки)|\d+(?=(\s+)?років))",result_list[5][1]):
                result_list[5][1] = re.findall(r"(\d+(?=(\s+)?рік)|\d+(?=(\s+)?роки)|\d+(?=(\s+)?років))",result_list[5][1])[0][0]
        else:
            result_list[5][1] = 0

    return result_list

def get_cards(page):
    try:
        titles = ("Досвід роботи","Освіта")
        raw_card_list = page.xpath("//div[@class='card card-indent wordwrap']/*")
        title = ""
        result_list = []
        for el in raw_card_list:
            if el.tag == "h2" and el.text in titles:
                title = el.text
                result_list.append([remove_double_whitespaces(el.text)])
            elif title and el.tag == "hr":
                title = ""
            elif title:
                for i in result_list:
                    if i[0] == title and el.xpath("./span"):
                        i.append(remove_double_whitespaces(el.xpath("./span")[0].text))
        temp = []
        for p in result_list:
            years,months=0,0
            try:
                if re.findall(r"(\d(?=(\s+)?рік)|\d(?=(\s+)?роки)|\d(?=(\s+)?років))", p[1], re.IGNORECASE):
                    years += int(re.findall(r"(\d(?=(\s+)?рік)|\d(?=(\s+)?роки)|\d(?=(\s+)?років))", p[1], re.IGNORECASE)[0][0])
                elif re.findall(r"(\d(?=( +)?місяці)|\d(?=( +)?місяців))", p[1], re.IGNORECASE):
                    months += int(re.findall(r"(\d(?=( +)?місяці)|\d(?=( +)?місяців))", p[1], re.IGNORECASE)[0][0])
            except IndexError:
                continue
            if years > 0 or months > 0:
                temp.append([p[0], years * 12 + months])
            else:
                temp.append([p[0], ""])  

        if len(temp) < 2:
            for i in range(0,2-len(temp)):
                temp.append((" "," "))
        return temp
    except Exception as e:
        print("get_cards fail ", e)

def format(row):
    fields = ("salary", "age")
    for i in row:
        if i[0] in fields:
            try:
                temp = ""
                if re.findall(r"(\d+.\d+){1,}(?= грн)",i[1]):
                    i[1] = int(re.sub(r"\s+","",re.findall(r"(\d+.\d+){1,}(?= грн)",i[1])[0]))
                    continue
                for j in re.findall(r"\d+",i[1]):
                    temp = temp + j
                    i[1] = int(temp)
            except Error as e: 
                print(e)
    return row 

def get_data(url,conn,cv_or_job): #use all functions on one job or cv page, insert row of results in db

    page = get_page(url)
    if cv_or_job == "cv":
        data_row = concat([["date", datetime.date.today().isoformat()]],format(get_xpath_data(page,cv_or_job)), get_cards(page),[['',url]])
    else:
        data_row = concat([["date", datetime.date.today().isoformat()]],format(get_xpath_data(page,cv_or_job)),[['',url]])
    if data_row[1] == "Особисті дані":
        data_row[1] =  ""
    insert(data_row,conn,cv_or_job)
    return True
    # except Exception as e:
    #     print("failed to extract data from ",cv_or_job, " page, ", e)

def insert(row, conn, cv_or_job):
    temp_row = [f[1] for f in row]
    print(temp_row)
    cur = conn.cursor()
    if cv_or_job == "cv":
        cur.execute(CV_INSERT_QUERY,temp_row)
    else:
        cur.execute(JOBS_INSERT_QUERY,temp_row)
    conn.commit()

def print_db(conn):
    cur = conn.cursor()
    cur.execute("SELECT * FROM CVs")
    b = cur.execute("SELECT * FROM JOBs")
    print(cur.fetchall())

def scrap(conn, cv_or_job, amount , scrap_all=False):
    rows_done = 0
    time_avg = datetime.timedelta()
    delay = 0.8
    pagenum = 1
    if cv_or_job == "cv":
        current_list_page_url = START_CV_LIST_PAGE_URL
    else:
        current_list_page_url = START_JOBS_LIST_PAGE_URL
    for pagenum in range(1,amount+1):
        time.sleep(delay)
        current_list_page_url = current_list_page_url.format(page_n=pagenum)
        current_url_list = get_data_page_urls_from_list_page(current_list_page_url,cv_or_job)
        for data_page_url in current_url_list:
            start_time = datetime.datetime.now()
            get_data(data_page_url, conn, cv_or_job)
            rows_done+=1
            time_avg = (time_avg + (datetime.datetime.now() - start_time))/rows_done
            delay = 3
            if rows_done >= amount and scrap_all==False:
                print("average time for ",cv_or_job, " is ", time_avg.seconds,"s ", time_avg.microseconds,"ms")
                return
            if check_list_page_if_last(current_list_page_url):
                print("average time for a",cv_or_job, " is ", time_avg.seconds,"s ", time_avg.microseconds,"ms")
                return
        pagenum += 1

def main():
    amount = 20 #how much datarows we want per day
    create_db()
    conn = new_connection(DB_PATH)
    # scrap(conn, "cv", amount, scrap_all=True)
    scrap(conn, "job", amount, scrap_all=False)
    conn.close()
    tor_request.close()

if __name__ == '__main__':
    main()