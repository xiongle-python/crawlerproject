import hashlib
import os
from random import random

import django
from django.core.mail import EmailMultiAlternatives
from django.shortcuts import render
from django.core.cache import cache
import re
from django.views.decorators.cache import cache_page

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "crawlerTwo.settings")
django.setup()

from redis import Redis

import happybase,MySQLdb,time,datetime
from django.core.paginator import Paginator
from django.http import HttpResponse, JsonResponse
from django.shortcuts import render, redirect

# Create your views here.
from crawlapp.models import User, Job
from crawlerTwo import settings


red = Redis(host="192.168.92.60",port=8000)
print(red)
def regist_page(request):
    """转发注册页面"""
    return render(request, "register.html")
# 注册逻辑
@cache_page(timeout=100,key_prefix="cacheRedis")
def regist_logic(request):
    username = request.POST.get("userid")
    usertel = request.POST.get("usrtel")
    # email = request.POST.get("email")
    password = request.POST.get("psw")
    print(username,usertel,password)
    try:
        if username:
            user = User(username=username, usertel=usertel,  password=password)
            user.save()
            # send_email(email,code)
            return render(request,"login.html")
        else:
            return render(request,"register.html")
    except Exception as e:
        print(e)

def login_page(request):
    return render(request,"login.html")
# 登录逻辑
@cache_page(timeout=100,key_prefix="cacheRedis")
def login_logic(request):
    """登录逻辑"""
    username = request.POST.get("userid")
    password = request.POST.get("psw")
    # request.session["username"] = username
    print(username, password)
    result=User.objects.filter(username=username)
    real_pwd=result[0].password
    print(real_pwd)
    if password==real_pwd:
        login_flag=result[0].username
        request.session['login_flag'] = login_flag
        return redirect("/admins/pac/main/")
    return render(request, "login.html")

def main(request):
    return render(request,"main.html")

def get_mysql(ID):
    """从mysql拿五页数据"""
    citys = ['北京', '上海', '广州', '深圳']
    dutys = ['python', '爬虫', '大数据','AI']
    cs, ds = citys[int(ID[0])], dutys[int(ID[1])]
    mys = Job.objects.filter(city=cs, duty__contains=ds)
    data = list(mys)
    return data


def get_hbase(ID):
    """从hbase拿数据"""
    table=get_conn_hb()
    citys = ['北京', '上海', '广州', '深圳']
    dutys = ['python', '爬虫', '大数据','AI']
    # print("RowFilter(=,'Regexstring:\.*%s\.*%s\.*')"% (citys[first],dutys[second]))
    # info = table.scan(filter="RowFilter(=,'Regexstring:\.*%s\.*%s\.*')"% (citys[first],dutys[second]))
    info_hb = table.scan(row_start=citys[int(ID[0])]+dutys[int(ID[1])], limit=3000)
    l, d = [], {}
    for key, value in info_hb:
        for key1, value1 in value.items():
            d.update({key1.decode("utf-8").split(":")[1]: value1.decode("utf-8")})
            l.append(d)
    count = len(l)
    print("listhbase=",l)
    return l,count


def get_hbasetwo(text):
    table=get_conn_hb()
    info_hb = table.scan(row_start=text, limit=3000)
    l,d = [],{}
    for key, value in info_hb:
        for key1, value1 in value.items():
            d.update({key1.decode("utf-8").split(":")[1]: value1.decode("utf-8")})
            l.append(d)
    count=len(l)
    return l ,count


# @cache_page(timeout=100,key_prefix="cacheRedis")
def get_page_list(request,info):
    """分页显示功能"""
    # user=User.objects.filter(username__exact=request.session.get("login_flag"))
    #设置访问次数和访问时长，若超过逾期，视为爬虫
    # count=user.values()[0]['count']+1
    # long=user.values()[0]['long']+3
    # if count>100:
    #     return redirect("/admins/main/deep/")
    # if long>60*5:
    #     return redirect("admins/main/long/")
    time.sleep(0.5)
    num_page = request.GET.get("num")
    if not num_page:
        num_page = 1
    num_page = int(num_page)
    page = Paginator(object_list=info, per_page=20).page(num_page)
    return page


def get_time():
    time2=time.asctime(time.localtime())
    return time2
def get_long(request):
    return render(request,"访问超时.html")
def get_deep(request):
    return render(request,"频率过高.html")



@cache_page(timeout=100,key_prefix="cacheRedis")
# def count_time(f):
#     def inner(*args):
#         start=time.clock()
#         r=f(*args)
#         end=time.clock()
#         # 用一个计时器，如果时间超过预设即跳转页面-------反爬之用
#         if end-start>=60*10:
#             return redirect("/amins/main/long/")
#         return r
#     return inner
# @count_time(method="get")
def list_page(request):
    """分页显示拿回的数据"""
    ID = request.GET.get("ID")
    if ID:
        request.session['ID']=ID
    if not ID:
        ID=request.session.get("ID")
    #从mysql拿，将queryset转为列表形式,先取出所有
    data=get_mysql(ID)
    # 后面数据从hbase拿
    list_hbase, count=get_hbase(ID)
    print("list_hb:",list_hbase)
    # 分页显示 以上输出的结果  若登陆显示所有结果
    list_all = data + list_hbase
    # 否则显示十页数据,卡下标每种只取五页，即100条数据
    list_five = data[0:100] + list_hbase[0:100]
    login_flag=request.session.get("login_flag")
    print("login=",login_flag)
    time1 = get_time()
    if login_flag:
        page=get_page_list(request,list_all)
        count = len(list_all)
        # 将日志信息存入hbase
        print("//",ID,time1,login_flag)
        build_user_log(ID, time1,login_flag)
    else:
        page=get_page_list(request,list_five)
        count = len(list_five)
        #获取用户ip和访问时间并入库
        ip=request.META["HTTP_HOST"]
        build_log_ip(ip,time1)
    return render(request, "menu.html", {"page": page, "count": count})

@cache_page(timeout=100,key_prefix="cacheRedis")
def search_list(request):
    """实现搜索功能"""
    select=request.GET.get("select")
    text = request.GET.get("text")
    print(select,text)
    hb_info,count = get_hbasetwo(text)
    print("hbinfo=",hb_info)
    if select=="1":
        mysql_info = list(Job.objects.filter(city__contains=text))
    else:
        mysql_info = list(Job.objects.filter(duty__contains=text))
    print("mysqlinfo=",mysql_info)
    login_flag = request.GET.get("login_flag")
    if not login_flag:
        list_all =  mysql_info[0:100]+hb_info[0:100]
    else:
        list_all =  mysql_info+hb_info
    count=len(list_all)
    def mydefault(u):
        if isinstance(u, Job):
            return {"duty": u.duty, "company": u.company, "address": u.address, "comscale": u.comscale,
                    "comnet": u.comnet, "city": u.city, "salary": u.salary}
    return JsonResponse({"list_all":list_all,"count":count},json_dumps_params={"default":mydefault})


def get_conn_hb():
    conn = happybase.Connection(host="192.168.92.60", port=9090)
    table = conn.table("Crawler_Info:51job")
    return table
def get_conn_mysql(sql,l):
    conn=MySQLdb.connect(host='127.0.0.1', port=3306,
            user='root', password='8911', charset='utf8', db='work')
    conn.cursor().execute(sql,l)
    conn.commit()

def build_user_log(ID,time,user):
    """构造用户登陆，浏览各方面信息的日志"""
    citys = ['北京', '上海', '广州', '深圳']
    dutys = ['python', '爬虫', '大数据','AI']
    city,duty=citys[int(ID[0])], dutys[int(ID[1])]
    print("??",city,duty,time,user)
    sql="insert into user_login(username,accesstime,access_city_duty)values (%s,%s,%s)"
    l=[user,time,city+duty]
    get_conn_mysql(sql,l)
def build_log_ip(ip,time):
    """用户未登陆log写入另一张表"""
    sql = "insert into user_unlogin(ip,accesstime)values (%s,%s)"
    l = [ip ,time]
    get_conn_mysql(sql, l)

def map(request):
    return render(request,"地图.html")
def bar(request):
    return render(request,"饼图.html")
def pie(request):
    return render(request,"柱状图.html")


#加盐给参数加密作反爬
def getsalt(pwd,salt=None):
    '''获取密码并变成盐'''
    h = hashlib.md5()
    if not salt:
        l = "gijhskdgjeuirtu658499848609/,./']["
        salt = ''.join(random.sample(l, 6)).replace(" ", "")
        password = pwd+salt
        h.update(password.encode())
        mypwd=h.hexdigest()
        return mypwd,salt
    else:
        password=pwd+salt
        h.update(password.encode())
        mypwd=h.hexdigest()
        return mypwd

def send_email(email, code):
    subject = '来自x719459355的注册确认邮件'
    text_content = '''欢迎你来验证你的邮箱，验证结束你就可以登录了！\如果你看到这条消息，说明你的邮箱服务器不提供HTML链接功能，请联系管理员！'''
    html_content = '''<p>感谢注册<a href="http://{}/test/confirm/?code={}"target=blank>www.baidu.com</a>，
    \ 欢迎你来验证你的邮箱，验证结束你就可以登录了！</p><p>请点击站点链接完成注册确认！</p>
    <p>此链接有效期为{}天！</p>
    '''.format('127.0.0.1:8000', code, settings.CONFIRM_DAYS)
    msg = EmailMultiAlternatives(subject, text_content, settings.EMAIL_HOST_USER, [email])
    msg.attach_alternative(html_content, "text/html")
    msg.send()







# def create_hbase_table():
#     #'连接hbase，并创建表，存入数据'
#     conn=happybase.Connection(host="192.168.92.60",port=9090)
#     # conn.open()
#     families={"uncommon":dict()}
#     conn.create_table("Crawler_Info:user_log",families)
# if __name__ == '__main__':
#     create_hbase_table()