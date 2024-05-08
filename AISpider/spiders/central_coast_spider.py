import re
import copy
import requests
import scrapy
import time
from datetime import datetime, timedelta
from scrapy import Request, Selector
from scrapy.http import HtmlResponse
from urllib.parse import urlencode
from http.cookies import SimpleCookie
from common._date import get_all_month_
from AISpider.items.central_coast_items import CentralCoastItem

DATE_FORMATE = "%d/%m/%Y"


def trans_str_2date(d: str, fmt=DATE_FORMATE):
    if d:
        try:
            return datetime.strptime(d, fmt)
        except:
            pass


class CentralCoastSpider(scrapy.Spider):
    name = "central_coast"
    allowed_domains = ["eservices.centralcoast.nsw.gov.au"]
    start_urls = ["https://eservices.centralcoast.nsw.gov.au/"]

    custom_settings = {
        # 'ITEM_PIPELINES': {
        #     "AISpider.pipelines.AispiderPipeline": None,
        # }
        'DOWNLOAD_DELAY': 3,
        'RANDOMIZE_DOWNLOAD_DELAY': True,
        'LOG_STDOUT': True,
        #'LOG_FILE': 'logs/central_coast.log',
        'DOWNLOAD_TIMEOUT': 300
    }

    type_select_url = 'https://eservices.centralcoast.nsw.gov.au/ePathway/Production/Web/GeneralEnquiry/EnquiryLists.aspx?ModuleCode=LAP'
    enquiry_search_url = 'https://eservices.centralcoast.nsw.gov.au/ePathway/Production/Web/GeneralEnquiry/EnquirySearch.aspx'
    enquiry_summary_url = 'https://eservices.centralcoast.nsw.gov.au/ePathway/Production/Web/GeneralEnquiry/EnquirySummaryView.aspx'
    applications_on_exhibition = 'Applications on exhibition'
    applications_determined = 'Applications Determined'
    applications_under_assessment = 'Applications Under Assessment'
    all_applications = "All Applications"
    application_enquiry_type = (
        # 这边提供 All Applications 选项
        # (applications_on_exhibition, 'ctl00$MainBodyContent$mDataList$ctl03$mDataGrid$ctl02$ctl00', 3),
        # (applications_determined, 'ctl00$MainBodyContent$mDataList$ctl03$mDataGrid$ctl03$ctl00', 2),
        # (applications_under_assessment, 'ctl00$MainBodyContent$mDataList$ctl03$mDataGrid$ctl04$ctl00', 1),
        (all_applications, 'ctl00$MainBodyContent$mDataList$ctl03$mDataGrid$ctl05$ctl00', 4),
    )

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Content-Type': 'application/x-www-form-urlencoded',
    }
    start_date = '31/12/2009'  # 查询的最早的时间

    def __init__(self, run_type='all', days=None, *args, **kwargs):
        """
        runtype: 指定爬虫的运行方式 all爬取一直以来所有数据， part指定date_runge爬取部分数据
        date_range:爬取数据的时间范围
        """
        super(CentralCoastSpider, self).__init__(*args, **kwargs)
        self.step = 1
        self.run_type = run_type
        if days == None:
            # 如果没有传days默认为这个月的数据
            self.days = get_this_month()
        else:
            now = datetime.now()
            days = int(days)
            date_from = (now - timedelta(days)).date().strftime('%d/%m/%Y')
            self.days = date_from
            # 这里计算出开始时间 设置到self.days
        self.previouspage = ''
        self.cookie = {}

    def set_cookies(self, enquiry_listid):  # ):
        r = requests.get(self.type_select_url)
        self.log(f'cookies:{r.cookies.items()}')
        self.cookie[enquiry_listid] = dict(r.cookies.items())  #
        return r

    def get_common_payload(self, respond, previouspage=False):
        params = {}
        selector = Selector(text=respond.text)
        params['__VIEWSTATE'] = selector.css('#__VIEWSTATE::attr(value)').get()
        params['__VIEWSTATEGENERATOR'] = selector.css('#__VIEWSTATEGENERATOR::attr(value)').get()
        t_previouspage = selector.css('input#__PREVIOUSPAGE::attr(value)').get()
        if t_previouspage:
            params['__PREVIOUSPAGE'] = t_previouspage
        params['__EVENTVALIDATION'] = selector.css('#__EVENTVALIDATION::attr(value)').get()
        # params['ctl00$CSRFToken'] = selector.css('input#CSRFToken::attr(value)').get()
        # print(params)
        return urlencode(params)

    def send_select_payload(self, element):
        params = {}
        params['__EVENTTARGET'] = ''
        params['__EVENTARGUMENT'] = ''
        params['__VIEWSTATEENCRYPTED'] = ''
        params['mDataGrid:Column0:Property'] = element
        params['ctl00$MainBodyContent$mContinueButton'] = 'Next'
        return urlencode(params)

    def get_query_payload(self, from_date=None, to_date=None, enquiry_listid=None, page=None, search_button=False,
                          menu=False):
        params = {}
        if menu:
            params['__EVENTTARGET'] = 'ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$tabControlMenu'
            params['__EVENTARGUMENT'] = 1
            params['ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$ctl04$mStreetNumberTextBox'] = ''
            params['ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$ctl04$mStreetNameTextBox'] = ''
            params['ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$ctl04$mStreetTypeDropDown'] = '(any)'
            params['ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$ctl04$mSuburbTextBox'] = ''
            params['hiddenInputToUpdateATBuffer_CommonToolkitScripts'] = 1
        else:
            params['__EVENTTARGET'] = '' if page is None else f'ctl00$MainBodyContent$mPagingControl$pageButton_{page}'
            params['__EVENTARGUMENT'] = ''
        params['__LASTFOCUS'] = ''
        params['ctl00$CSRFToken'] = ''
        if enquiry_listid is not None:
            params['ctl00$MainBodyContent$mGeneralEnquirySearchControl$mEnquiryListsDropDownList'] = str(enquiry_listid)
        if from_date:
            params[
                'ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$ctl09$mFromDatePicker$dateTextBox'] = from_date
        if to_date:
            params[
                'ctl00$MainBodyContent$mGeneralEnquirySearchControl$mTabControl$ctl09$mToDatePicker$dateTextBox'] = to_date
        if search_button:
            params['ctl00$MainBodyContent$mGeneralEnquirySearchControl$mSearchButton'] = 'Search'
        params['ctl00$mWidth'] = 510
        params['ctl00$mHeight'] = 936
        # print(params)
        return urlencode(params)

    def parse(self, response):
        """
        在这里进行查询
        """
        for _, element, enquirylistid in self.application_enquiry_type:
            # 第一次请求,获取cookie
            # 发送选择
            r = self.set_cookies(enquirylistid)
            headers = copy.copy(self.headers)
            payload = '&'.join([self.get_common_payload(r), self.send_select_payload(element)])
            r = requests.post(url=self.type_select_url, headers=headers, data=payload,
                              cookies=self.cookie[enquirylistid])
            s_response = HtmlResponse(url=r.url, body=r.text, encoding='utf-8', request=r.request)
            enquiry_listid = enquirylistid
            now_date = datetime.now()

            if self.run_type == 'all':
                years = get_all_month_(self.days, datetime.now().date().strftime('%d/%m/%Y'))
                #查询所有记录, 这个城市记录较少，所以可以每10年查找一次
                # years = [y for y in list(range(start_date.year, now_date.year, self.step))]
                # if now_date.year not in years:
                #     years = [f'01/01/{y}' for y in
                #              list(range(start_date.year, now_date.year, self.step)) + [now_date.year]]
                # else:
                #     years = [f'01/01/{y}' for y in list(range(start_date.year, now_date.year, self.step))]
                for idx, y in enumerate(years):
                    if y == years[-1]:
                        break
                    self.logger.info(f'from_date:{y}, to_date:{years[idx + 1]}')
                    page = 1
                    payload = '&'.join(
                        [self.get_common_payload(s_response, previouspage=(True if idx != 0 else False)),
                         self.get_query_payload(enquiry_listid=enquiry_listid, menu=True)])
                    # 需要先请求menu，然后在搜索，否则返回所有数据
                    if idx < 2:
                        headers = copy.copy(self.headers)
                        url = self.enquiry_summary_url if idx != 0 else self.enquiry_search_url
                        r = requests.post(url, headers=headers, data=payload, cookies=self.trans_cookie_todict(
                            s_response.request.headers.get('cookie').encode('utf-8')))  # [enquiry_listid]
                        s_response = HtmlResponse(url=r.url, body=r.text, encoding='utf-8', request=r.request)
                    # 这块需要判断html请求是不是有数据，或者异常，所以不能使用scrapy.Request,因为yield 会让程序继续执行无法判断是否需要继续循环
                    headers = copy.copy(self.headers)
                    payload = '&'.join(
                        [self.get_common_payload(s_response, previouspage=(True if idx != 0 else False)),
                         self.get_query_payload(y, years[idx + 1], enquiry_listid, search_button=True)])
                    r = requests.post(url, headers=headers, data=payload, cookies=self.trans_cookie_todict(
                        s_response.request.headers.get('cookie').encode('utf-8')))  # [enquiry_listid]
                    s_response = HtmlResponse(url=r.url, body=r.text, encoding='utf-8', request=r.request)
                    # 判断是否session过期
                    session_expired = s_response.css(
                        'div#ctl00_MainBodyContent_mErrorPanel legend::text').extract_first()
                    if session_expired and session_expired.strip() == 'Session Expired':
                        raise Exception(f'Session Expired\n{url}')
                    # 判断是否没记录
                    no_record = s_response.css('span#ctl00_MainBodyContent_mNoRecordLabel')
                    if no_record:
                        continue
                    #  解析列表
                    for item in self.parse_grid(s_response, enquiry_listid):
                        yield item
                    # 获取总页数
                    page_str = s_response.css(
                        'span#ctl00_MainBodyContent_mPagingControl_pageNumberLabel::text').extract_first()
                    total = int(re.findall(r'\d+', page_str)[-1]) if page_str else -1

                    while page < total:
                        # 如果结果有分页， 在循环中获取其他页面
                        page += 1
                        url = '?'.join([self.enquiry_summary_url, urlencode({'PageNumber': page})])
                        # headers = copy.copy(self.headers)
                        payload = '&'.join(
                            [self.get_common_payload(s_response, previouspage=True),
                             self.get_query_payload(y, years[idx + 1], enquiry_listid, page)])
                        # headers |= {'Content-Type': 'application/x-www-form-urlencoded'}
                        # 这块需要判断html请求是不是有数据，或者异常，所以不能使用scrapy.Request,因为yield 会让程序继续执行无法判断是否需要继续循环
                        r = requests.post(url, data=payload, headers=self.headers, cookies=self.trans_cookie_todict(
                            s_response.request.headers.get('cookie').encode('utf=8')))  # [enquiry_listid]
                        s_response = HtmlResponse(url=r.url, body=r.text, encoding='utf-8', request=r.request)
                        # 判断是否session过期
                        session_expired = s_response.css(
                            'div#ctl00_MainBodyContent_mErrorPanel legend::text').extract_first()
                        if session_expired and session_expired.strip() == 'Session Expired':
                            # self.logger.error(f'Session Expired\n{url}')
                            raise Exception(f'Session Expired\n{url}')
                        #  解析列表
                        for item in self.parse_grid(s_response, enquiry_listid):
                            yield item

    def trans_cookie_todict(self, r_b_cookie):
        cookie = SimpleCookie()
        cookie.load(r_b_cookie.decode('utf-8'))
        return {key: morsel.value for key, morsel in cookie.items()}

    def parse_grid(self, respond: HtmlResponse, query_list_id: str):
        """
        解析表格页面
        """
        rows = respond.css('table#gridResults tr')
        for row in rows[1:]:
            # 获取链接中的id
            link = row.css('td a::attr(href)').extract_first()
            # 获取app_num
            app_id = re.search('Id=(\d+)', link) or None
            if app_id:
                app_id = app_id.group(1)
            # 获取app详情页
            url = '/'.join([respond.url.rsplit('/', 1)[0], link], )
            yield Request(url, dont_filter=True,
                          meta={'app_id': app_id, 'app_num': row.css('td a::text').extract_first()},
                          cookies=self.trans_cookie_todict(respond.request.headers.get('cookie').encode('utf-8')),
                          callback=self.parse_detail)

    def parse_detail(self, respond: HtmlResponse):
        """
        访问详情页， 获取所有信息
        """
        item = CentralCoastItem()
        item['application_id'] = respond.meta.get('app_id')
        item['application_num'] = respond.meta.get('app_num')
        # details
        item['description'] = respond.css(
            'div[id^=ctl00_MainBodyContent_DynamicField_Application_Description] div.AlternateContentText::text').extract_first()
        try:
            lodged_date = respond.css(
            'div[id^=ctl00_MainBodyContent_DynamicField_Lodgement_Date] div.AlternateContentText::text').extract_first()
            time_array = time.strptime(lodged_date, '%d/%m/%Y')
            temp_data = int(time.mktime(time_array))
            item["lodgement_date"] = temp_data if lodged_date else 0
        except:
            item["lodgement_date"]=0
        # item['lodgement_date'] = trans_str_2date(respond.css(
        #     'div[id^=ctl00_MainBodyContent_DynamicField_Lodgement_Date] div.AlternateContentText::text').extract_first())
        
        item['status'] = respond.css(
            'div[id^=ctl00_MainBodyContent_DynamicField_Status] div.AlternateContentText::text').extract_first()
        item['responsible_officer'] = respond.css(
            'div[id^=ctl00_MainBodyContent_DynamicField_Responsible_Officer] div.AlternateContentText::text').extract_first()
        item['address'] = respond.css(
            'div[id^=ctl00_MainBodyContent_DynamicGroup_Property_Details] table#gridResults tr.ContentPanel span::text').extract_first()

        # decision
        decisions = []
        application_decision = respond.css(
            'div[id^=ctl00_MainBodyContent_DynamicGroup_Decision_Details] table#gridResults tr')
        if len(application_decision) > 1:
            item['decision'] = application_decision[1].css('td div::text').extract_first()
            try:
                lodged_date = application_decision[1].css('td span::text').extract_first()
                time_array = time.strptime(lodged_date, '%d/%m/%Y')
                temp_data = int(time.mktime(time_array))
                item["decision_date"] = temp_data if lodged_date else 0
            except:
                item["decision_date"]=0


        # Applicant
        applicants = respond.css(
            'div[id^=ctl00_MainBodyContent_DynamicGroup_Name_Details] table#gridResults td div::text').extract()
        item['names'] = ';'.join(applicants)

        # documents
        # documents_url = respond.css('a#ctl00_MainBodyContent_mHyperLinkAttachments::attr(href)').extract_first()
        # if documents_url:
        #     documents_list = []
        #     documents_url = ''.join([self.start_urls[0][:-1], documents_url])
        #     r = requests.post(documents_url, headers=self.headers,
        #                       cookies=self.trans_cookie_todict(respond.request.headers.get('cookie')))
        #     document_selector = Selector(text=r.text)
        #     documents = document_selector.css('table.AlternateContentPanel tr')
        #     if documents:
        #         for document in documents[1:]:
        #             attachment_type, plans = document.css('td::text').extract()[:2]
        #             path = document.css('td a::attr(href)').extract_first()
        #             file_url = ''.join([self.start_urls[0][:-1], path])
        #             documents_list.append('@@'.join([file_url, attachment_type, plans]))
        item['documents'] = \
            f"https://datracker.centralcoast.nsw.gov.au/webgrid/?s=WebGridPublishingDA&container={item['application_num']}"
        item['metadata'] ={}
        del item['metadata']
        yield item
