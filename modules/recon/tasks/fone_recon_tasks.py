"""FONE 往来对账 - API 调用 Tasks

封装 FONE 系统的登录和脚本执行接口，供 Prefect Flow 调用。
"""

import json
import os
import sys
from typing import Any, Dict

import requests

from prefect import task

sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))

FONE_LOGIN_URL = "http://10.18.6.222"
FONE_SCRIPT_URL = "https://epmtest.xgd.com:80"
USERNAME = "songsong"
PASSWORD = "uenmdk@#cjdfk234kfj#@ff"

APP_ID = "68abd59a511df11f7f9c8b5c"
APP_USER_ID = "68abd59a511df11f7f9c8cf8"
GLOBAL_USER_ID = "63f5ba3e6c02b6593662e459"


def _build_script_text(start_date: str, end_date: str) -> str:
    """构造 0501 脚本内容，替换日期变量。"""
    return (
        "var 开始日期='" + start_date + "';"
        "var 结束日期='" + end_date + "';"
        "var 科目余额表='66f36837f6f9db36f4cc9885';"
        "var v0506科目余额表写入数据中心='66f51592b96f29622372eace';"
        "var v0508推BI的内部关联方数据='689d8dc6a684ba0ce608b775';"
        "var newdate = formate(new Date());\r\n"
        "var token = getToken(); \r\n"
        "var url = 'http://dc-api.xgd.com/xgd-erp-fone-service/api/standardWebApi';\r\n"
        "var header_common = {\r\n"
        "    'Content-Type':'application/x-www-form-urlencoded',\r\n"
        "    'Authorization':'Bearer '+ token\r\n"
        "};\r\n"
        "var body_GL_RPT_AccountBalance = {\r\n"
        "    'deal':'GET_SYS_REPORT_DATA',\r\n"
        "    'formId':'GL_RPT_AccountBalance',\r\n"
        "    'data':'',\r\n"
        "};\r\n"
        "\r\n"
        "var SQL_GetOrgList = '  select org.FNUMBER ACCOUNTORGNUMBER ,b.FBOOKID ,b.FNUMBER,orgl.FNAME '\r\n"
        "                    +'from T_BD_ACCOUNTBOOK b '\r\n"
        "                    +'left outer join T_ORG_ORGANIZATIONS org on org.FORGID = b.FACCOUNTORGID '\r\n"
        "                    +'left outer join T_ORG_ORGANIZATIONS_L orgl on org.FORGID = orgl.FORGID and orgl.FLOCALEID = 2052 '\r\n"
        "                    +'where b.FINITIALSTATUS = \\u00271\\u0027  --只取已完成初始化的账套' ;\r\n"
        "var AccountOrgList  = getSqlData_ERP(SQL_GetOrgList);\r\n"
        "\r\n"
        "XAPI.ClearSheetData(科目余额表,0);\r\n"
        "\r\n"
        "\r\n"
        "// 获取开始的年和月\r\n"
        "var BegDate = new Date(开始日期);\r\n"
        "let currentYear = BegDate.getFullYear();\r\n"
        "let currentMonth = BegDate.getMonth(); // 月份从0开始，0表示1月\r\n"
        "// 获取结束的年和月\r\n"
        "var EndDate = new Date(结束日期);\r\n"
        "const endYear = EndDate.getFullYear();\r\n"
        "const endMonth = EndDate.getMonth();\r\n"
        "\r\n"
        "// 循环按年和月遍历\r\n"
        "while (currentYear < endYear || (currentYear === endYear && currentMonth <= endMonth)) {\r\n"
        "    // 输出当前的年和月\r\n"
        "    console.log(currentYear*100+(currentMonth+1));\r\n"
        "    var YYYY = currentYear;\r\n"
        "    var MM = (currentMonth+1);\r\n"
        "    \r\n"
        "    //Beg-取当月所有法人组织数据\r\n"
        "    for(let org in AccountOrgList){\r\n"
        "        var startRow_GL_RPT_AccountBalance = 0;\r\n"
        "        var loopLen_GL_RPT_AccountBalance = 1;\r\n"
        "        var params_GL_RPT_AccountBalance = {\r\n"
        "            'FieldKeys': 'FBALANCEID,FBALANCENAME,FDETAILNUMBER,FDETAILNAME,FBEGINDEBIT,FBEGINDEBITLOCAL,FBEGINCREDIT,FBEGINCREDITLOCAL,FDEBIT,FDEBITLOCAL,FCREDIT,FCREDITLOCAL,FYTDDEBIT,FYTDDEBITLOCAL,FYTDCREDIT,FYTDCREDITLOCAL,FENDDEBIT,FENDDEBITLOCAL,FENDCREDIT,FENDCREDITLOCAL',\r\n"
        "            'SchemeId': '',\r\n"
        "            'StartRow': startRow_GL_RPT_AccountBalance,\r\n"
        "            'IsVerifyBaseDataField': 'true',\r\n"
        "            'FilterString': [],\r\n"
        "            'Model': {\r\n"
        "                'FACCTBOOKID': {\r\n"
        "                    'FNumber': AccountOrgList[org]['FNUMBER']\r\n"
        "                },\r\n"
        "                'FCURRENCY': '0',\r\n"
        "                'FSTARTYEAR': YYYY,\r\n"
        "                'FSTARTPERIOD': MM,\r\n"
        "                'FENDYEAR': YYYY,\r\n"
        "                'FENDPERIOD': MM,\r\n"
        "                'FBALANCELEVEL': '4',\r\n"
        "                'FSTARTBALANCE': {\r\n"
        "                    'FNumber': ''\r\n"
        "                },\r\n"
        "                'FENDBALANCE': {\r\n"
        "                    'FNumber': ''\r\n"
        "                },\r\n"
        "                'FSHOWDETAIL': true,/*显示核算维度明细*/\r\n"
        "                'FFORBIDBALANCE': true,/*显示禁用科目 */\r\n"
        "                'FNOTPOSTVOUCHER': true,/*包括未过账凭证*/\r\n"
        "                'FDEBITORCREDIT': false,/*余额按借方、贷方分别小计*/\r\n"
        "                'FBALANCEZERO': true,/*包括余额为零的科目*/\r\n"
        "                'FNOBUSINESS': false,/* 包括没有业务发生的科目（期初、本年累计）*/\r\n"
        "                'FPERIODNOBALANCE': true,/*包括本期没有发生额的科目*/\r\n"
        "                'FYEARNOBALANCE': true,/*包括本年没有发生额的科目*/\r\n"
        "                'FSHOWFULLNAME': true,/*显示科目全名*/\r\n"
        "                'FDETAILSHOWACCT': true,/*核算维度明细行显示科目信息*/\r\n"
        "                'FSHOWDETAILONLY': true,/*只显示明细科目*/\r\n"
        "                'FEXCLUDEADJUSTVCH': false,/* 不包含调整期凭证*/\r\n"
        "                'FFLEXDEBITORCREDIT': false,/*核算维度余额按借方、贷方分别小计*/\r\n"
        "                'FSHOWFLEXBYCOL': false/*核算维度分列显示*/\r\n"
        "            }\r\n"
        "        };\r\n"
        "        body_GL_RPT_AccountBalance.data = JSON.stringify(params_GL_RPT_AccountBalance);\r\n"
        "        var dataResult_GL_RPT_AccountBalance = XAPI.HttpPostFormData(url, '', '', JSON.stringify(body_GL_RPT_AccountBalance), JSON.stringify(header_common));\r\n"
        "        var rowCount_GL_RPT_AccountBalance = JSON.parse(dataResult_GL_RPT_AccountBalance).data.Result.RowCount;\r\n"
        "        var dataRows_GL_RPT_AccountBalance = JSON.parse(dataResult_GL_RPT_AccountBalance).data.Result.Rows;\r\n"
        "        var datas_GL_RPT_AccountBalance = [];\r\n"
        "        if(rowCount_GL_RPT_AccountBalance>0){\r\n"
        "            for (var d in dataRows_GL_RPT_AccountBalance){\r\n"
        "                var data = {\r\n"
        "                    'YYYY':''\r\n"
        "                    ,'MM':''\r\n"
        "                    ,'OrgCode':''\r\n"
        "                    ,'OrgName':''\r\n"
        "                    ,'FBALANCEID':''\r\n"
        "                    ,'FBALANCENAME':''\r\n"
        "                    ,'FDETAILNUMBER':''\r\n"
        "                    ,'FDETAILNAME':''\r\n"
        "                    ,'FBEGINDEBIT':''\r\n"
        "                    ,'FBEGINDEBITLOCAL':''\r\n"
        "                    ,'FBEGINCREDIT':''\r\n"
        "                    ,'FBEGINCREDITLOCAL':''\r\n"
        "                    ,'FDEBIT':''\r\n"
        "                    ,'FDEBITLOCAL':''\r\n"
        "                    ,'FCREDIT':''\r\n"
        "                    ,'FCREDITLOCAL':''\r\n"
        "                    ,'FYTDDEBIT':''\r\n"
        "                    ,'FYTDDEBITLOCAL':''\r\n"
        "                    ,'FYTDCREDIT':''\r\n"
        "                    ,'FYTDCREDITLOCAL':''\r\n"
        "                    ,'FENDDEBIT':''\r\n"
        "                    ,'FENDDEBITLOCAL':''\r\n"
        "                    ,'FENDCREDIT':''\r\n"
        "                    ,'FENDCREDITLOCAL':''\r\n"
        "                };\r\n"
        "                data.YYYY = YYYY;\r\n"
        "                data.MM = MM;\r\n"
        "                data.OrgCode = AccountOrgList[org]['FNUMBER'];\r\n"
        "                data.OrgName = AccountOrgList[org]['FNAME'];\r\n"
        "                data.FBALANCEID = dataRows_GL_RPT_AccountBalance[d]['0'];\r\n"
        "                data.FBALANCENAME = dataRows_GL_RPT_AccountBalance[d]['1'];\r\n"
        "                \r\n"
        "                data.FDETAILNUMBER = dataRows_GL_RPT_AccountBalance[d]['2'];\r\n"
        "                data.FDETAILNAME = dataRows_GL_RPT_AccountBalance[d]['3'];\r\n"
        "                \r\n"
        "                data.FBEGINDEBIT = dataRows_GL_RPT_AccountBalance[d]['4'];\r\n"
        "                data.FBEGINDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['5'];\r\n"
        "                data.FBEGINCREDIT = dataRows_GL_RPT_AccountBalance[d]['6'];\r\n"
        "                data.FBEGINCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['7'];\r\n"
        "                data.FDEBIT = dataRows_GL_RPT_AccountBalance[d]['8'];\r\n"
        "                data.FDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['9'];\r\n"
        "                data.FCREDIT = dataRows_GL_RPT_AccountBalance[d]['10'];\r\n"
        "                data.FCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['11'];\r\n"
        "                data.FYTDDEBIT = dataRows_GL_RPT_AccountBalance[d]['12'];\r\n"
        "                data.FYTDDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['13'];\r\n"
        "                data.FYTDCREDIT = dataRows_GL_RPT_AccountBalance[d]['14'];\r\n"
        "                data.FYTDCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['15'];\r\n"
        "                data.FENDDEBIT = dataRows_GL_RPT_AccountBalance[d]['16'];\r\n"
        "                data.FENDDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['17'];\r\n"
        "                data.FENDCREDIT = dataRows_GL_RPT_AccountBalance[d]['18'];\r\n"
        "                data.FENDCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['19'];\r\n"
        "                datas_GL_RPT_AccountBalance.push(data);\r\n"
        "            }\r\n"
        "            while((10000*loopLen_GL_RPT_AccountBalance + 1) < rowCount_GL_RPT_AccountBalance){\r\n"
        "                startRow_GL_RPT_AccountBalance = 10000*loopLen_GL_RPT_AccountBalance + 1;\r\n"
        "                params_GL_RPT_AccountBalance.StartRow = startRow_GL_RPT_AccountBalance;\r\n"
        "                body_GL_RPT_AccountBalance.data = JSON.stringify(params_GL_RPT_AccountBalance);\r\n"
        "                dataResult_GL_RPT_AccountBalance = XAPI.HttpPostFormData(url, '', '', JSON.stringify(body_GL_RPT_AccountBalance), JSON.stringify(header_common));\r\n"
        "                dataRows_GL_RPT_AccountBalance = JSON.parse(dataResult_GL_RPT_AccountBalance).data.Result.Rows;\r\n"
        "                for (var d in dataRows_GL_RPT_AccountBalance){\r\n"
        "                    var data = {\r\n"
        "                        'YYYY':''\r\n"
        "                        ,'MM':''\r\n"
        "                        ,'OrgCode':''\r\n"
        "                        ,'FBALANCEID':''\r\n"
        "                        ,'FBALANCENAME':''\r\n"
        "                        ,'FDETAILNUMBER':''\r\n"
        "                        ,'FDETAILNAME':''\r\n"
        "                        ,'FBEGINDEBIT':''\r\n"
        "                        ,'FBEGINDEBITLOCAL':''\r\n"
        "                        ,'FBEGINCREDIT':''\r\n"
        "                        ,'FBEGINCREDITLOCAL':''\r\n"
        "                        ,'FDEBIT':''\r\n"
        "                        ,'FDEBITLOCAL':''\r\n"
        "                        ,'FCREDIT':''\r\n"
        "                        ,'FCREDITLOCAL':''\r\n"
        "                        ,'FYTDDEBIT':''\r\n"
        "                        ,'FYTDDEBITLOCAL':''\r\n"
        "                        ,'FYTDCREDIT':''\r\n"
        "                        ,'FYTDCREDITLOCAL':''\r\n"
        "                        ,'FENDDEBIT':''\r\n"
        "                        ,'FENDDEBITLOCAL':''\r\n"
        "                        ,'FENDCREDIT':''\r\n"
        "                        ,'FENDCREDITLOCAL':''\r\n"
        "                    };\r\n"
        "                    data.YYYY = YYYY;\r\n"
        "                    data.MM = MM;\r\n"
        "                    data.OrgCode = AccountOrgList[org]['FNUMBER'];\r\n"
        "                    data.OrgName = AccountOrgList[org]['FNAME'];\r\n"
        "                    data.FBALANCEID = dataRows_GL_RPT_AccountBalance[d]['0'];\r\n"
        "                    data.FBALANCENAME = dataRows_GL_RPT_AccountBalance[d]['1'];\r\n"
        "                    data.FDETAILNUMBER = dataRows_GL_RPT_AccountBalance[d]['2'];\r\n"
        "                    data.FDETAILNAME = dataRows_GL_RPT_AccountBalance[d]['3'];\r\n"
        "                    data.FBEGINDEBIT = dataRows_GL_RPT_AccountBalance[d]['4'];\r\n"
        "                    data.FBEGINDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['5'];\r\n"
        "                    data.FBEGINCREDIT = dataRows_GL_RPT_AccountBalance[d]['6'];\r\n"
        "                    data.FBEGINCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['7'];\r\n"
        "                    data.FDEBIT = dataRows_GL_RPT_AccountBalance[d]['8'];\r\n"
        "                    data.FDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['9'];\r\n"
        "                    data.FCREDIT = dataRows_GL_RPT_AccountBalance[d]['10'];\r\n"
        "                    data.FCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['11'];\r\n"
        "                    data.FYTDDEBIT = dataRows_GL_RPT_AccountBalance[d]['12'];\r\n"
        "                    data.FYTDDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['13'];\r\n"
        "                    data.FYTDCREDIT = dataRows_GL_RPT_AccountBalance[d]['14'];\r\n"
        "                    data.FYTDCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['15'];\r\n"
        "                    data.FENDDEBIT = dataRows_GL_RPT_AccountBalance[d]['16'];\r\n"
        "                    data.FENDDEBITLOCAL = dataRows_GL_RPT_AccountBalance[d]['17'];\r\n"
        "                    data.FENDCREDIT = dataRows_GL_RPT_AccountBalance[d]['18'];\r\n"
        "                    data.FENDCREDITLOCAL = dataRows_GL_RPT_AccountBalance[d]['19'];\r\n"
        "                    datas_GL_RPT_AccountBalance.push(data);\r\n"
        "                }\r\n"
        "                loopLen_GL_RPT_AccountBalance = loopLen_GL_RPT_AccountBalance +1;\r\n"
        "            }\r\n"
        "            XAPI.UpdateFormFromJson(JSON.stringify(datas_GL_RPT_AccountBalance),科目余额表, 0, 0);\r\n"
        "        }\r\n"
        "    }\r\n"
        "    \r\n"
        "    // 增加一个月\r\n"
        "    currentMonth++;\r\n"
        "    if (currentMonth > 11) { // 月份超过12月\r\n"
        "        currentMonth = 0; // 重置为1月\r\n"
        "        currentYear++; // 年份增加\r\n"
        "    }\r\n"
        "}\r\n"
        "\r\n"
        "XAPI.ExecuteDataStream(v0506科目余额表写入数据中心);\r\n"
        "var re = XAPI.ExecuteDataStream(v0508推BI的内部关联方数据);\r\n"
        "console.log(re);\r\n"
        "\r\n"
        "var url = 'http://10.18.10.71:3001/api/fone/wecom/send';\r\n"
        "var header = JSON.stringify({\r\n"
        "    'Content-Type': 'application/json',\r\n"
        "    'Authorization': 'Bearer ' + token\r\n"
        "});\r\n"
        "\r\n"
        "// 自动判断成功/失败文案\r\n"
        "var content = re && re.includes('0508-推BI的内部关联方数据运行结束')\r\n"
        "    ? newdate + '  脚本运行成功'\r\n"
        "    : newdate + '  脚本运行失败';\r\n"
        "\r\n"
        "// 构造请求体\r\n"
        "var body = JSON.stringify({\r\n"
        "    'optSystem': 'Fone',\r\n"
        "    'businessType': 'send',\r\n"
        "    'text': {\r\n"
        "        'toUsers': ['lijie01@xgd.com','songsong@xgd.com'],\r\n"
        "        'content': content\r\n"
        "    }\r\n"
        "});\r\n"
        "// 发送企微\r\n"
        "var dataResult = XAPI.HttpPost(url, '', '', body, header);\r\n"
        "console.log(content);\r\n"
        "console.log(dataResult);\r\n"
        "\r\n"
        "\r\n"
        "//作用：获取金蝶SQL返回的结果，Sql：查询语句\r\n"
        "function getSqlData_ERP(Sql){\r\n"
        "    var dataResult = XAPI.ExecuteDBQuery(\r\n"
        "        'SQL SERVER'\r\n"
        "        ,'10.18.4.27'\r\n"
        "        ,1433\r\n"
        "        ,'fone'\r\n"
        "        ,'xw@!YMb9r'\r\n"
        "        ,'AIS20200327122556'\r\n"
        "        ,Sql\r\n"
        "    );\r\n"
        "    var datas = JSON.parse(dataResult).data;\r\n"
        "    return datas;\r\n"
        "}\r\n"
        "\r\n"
        "//作用：获取token\r\n"
        "function getToken(){\r\n"
        "    /*Beg 接口认证，获取token*/\r\n"
        "    var tokenurl = 'http://dc-api.xgd.com/oauth-service/oauth/token';\r\n"
        "    var username = '71291406-BFC2-4A60-8B12-296744087A6E';\r\n"
        "    var password = 'akkvx4BG1OhNqOZpMHaAhA==';\r\n"
        "    var tokenheader = {'Content-Type':'application/x-www-form-urlencoded'};\r\n"
        "    var tokenbody = {\r\n"
        "        'username':username,\r\n"
        "        'password':password\r\n"
        "    };\r\n"
        "    var tokendataResult = XAPI.HttpPostFormData(tokenurl, '', '', JSON.stringify(tokenbody), JSON.stringify(tokenheader));\r\n"
        "    var token = JSON.parse(tokendataResult).data.accessToken;\r\n"
        "    /*End 接口认证，获取token*/\r\n"
        "    return token;\r\n"
        "}\r\n"
        "\r\n"
        "function formate(date){\r\n"
        "    //获取年月日，时间\r\n"
        "    var year = date.getFullYear();\r\n"
        "    var mon = (date.getMonth()+1) < 10 ? '0'+(date.getMonth()+1) : date.getMonth()+1;\r\n"
        "    var datea = date.getDate()  < 10 ? '0'+(date.getDate()) : date.getDate();\r\n"
        "    var hour = date.getHours()  < 10 ? '0'+(date.getHours()) : date.getHours();\r\n"
        "    var min =  date.getMinutes()  < 10 ? '0'+(date.getMinutes()) : date.getMinutes();\r\n"
        "    var seon = date.getSeconds() < 10 ? '0'+(date.getSeconds()) : date.getSeconds();\r\n"
        "                     \r\n"
        "    var newdatee = year +'-'+ mon +'-'+ datea +' '+ hour +':'+ min +':'+ seon;\r\n"
        "    return newdatee;\r\n"
        "}"
    )


@task(name="get_fone_token", log_prints=True)
def get_fone_token_task() -> Dict[str, str]:
    """调用 FONE 登录接口获取 ticket 和 user_id。"""
    url = f"{FONE_LOGIN_URL}/api/login/test"
    payload = {"username": USERNAME, "password": PASSWORD}
    headers = {"Content-Type": "application/json"}

    print(f"--> 请求登录接口: {url}")
    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=30)
    except requests.exceptions.Timeout:
        raise RuntimeError("登录请求超时 - FONE 服务器可能无法访问")
    except requests.exceptions.ConnectionError as e:
        raise RuntimeError(f"连接失败 - 无法连接到 FONE 服务器 ({FONE_LOGIN_URL}): {e}")

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"登录响应解析失败 (HTTP {resp.status_code}): {e}, 原始内容: {resp.text}")

    print(f"<-- 登录响应 (HTTP {resp.status_code}):")
    print(json.dumps(data, ensure_ascii=False, indent=2))

    if not data.get("isSuccess"):
        raise RuntimeError(f"登录失败: {data}")

    ticket = data.get("data", {}).get("ticket") or data.get("data", {}).get("Ticket")
    user_id = data.get("data", {}).get("user_id")
    if not ticket:
        raise RuntimeError(f"登录响应中未找到 ticket: {data}")

    print(f"✓ 登录成功！获取 ticket 成功")
    return {"ticket": ticket, "user_id": user_id}


@task(name="execute_fone_recon_script", log_prints=True)
def execute_fone_recon_script_task(ticket: str, start_date: str, end_date: str) -> Dict[str, Any]:
    """调用 FONE 执行脚本接口，运行 0501 往来对账脚本。"""
    url = f"{FONE_SCRIPT_URL}/api/Script/ExcuteScriptText"
    headers = {
        "Content-Type": "application/json",
        "ewaresoft-fone-applicationid": APP_ID,
        "ewaresoft-fone-applicationuserid": APP_USER_ID,
        "ewaresoft-fone-globaluserid": GLOBAL_USER_ID,
        "Authorization": ticket,
    }

    script_text = _build_script_text(start_date, end_date)

    payload = {
        "appID": APP_ID,
        "appUserId": APP_USER_ID,
        "scriptText": script_text,
        "context": "",
        "fContentId": "66f3691df6f9db36f4cc9e32",
        "taskId": "script_prefect_" + str(int(__import__("time").time())),
        "scriptName": "0501-获取ERP科目余额表-WebApi",
    }

    print(f"\n--> 请求执行脚本接口: {url}")
    print(f"--> 脚本日期范围: {start_date} ~ {end_date}")

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=300)
    except requests.exceptions.Timeout:
        raise RuntimeError("执行脚本请求超时 (300s)")
    except Exception as e:
        raise RuntimeError(f"执行脚本请求异常: {e}")

    print(f"<-- 执行脚本响应 (HTTP {resp.status_code})")

    try:
        data = resp.json()
    except Exception as e:
        raise RuntimeError(f"执行脚本响应解析失败: {e}, 原始内容: {resp.text}")

    print(json.dumps(data, ensure_ascii=False, indent=2))

    if not data.get("isSuccess"):
        raise RuntimeError(f"执行脚本接口调用失败: {data}")

    inner_data_str = data.get("data", "{}")
    try:
        inner_data = json.loads(inner_data_str)
    except json.JSONDecodeError:
        inner_data = {"raw": inner_data_str}

    script_status = inner_data.get("status")
    console_logs = inner_data.get("consoleLogs", [])
    error_messages = inner_data.get("errorMessage", [])

    print(f"\n✓ 脚本执行 API 调用成功")
    print(f"--> 脚本内部状态: {script_status}")
    print(f"--> 控制台日志条数: {len(console_logs)}")

    if console_logs:
        print("--> 控制台日志:")
        for log in console_logs:
            print(f"    {log}")

    if error_messages:
        print(f"--> 错误信息: {error_messages}")
        raise RuntimeError(f"脚本执行返回错误: {error_messages}")

    if script_status != 0:
        raise RuntimeError(f"脚本内部状态非成功: status={script_status}")

    return {
        "api_success": True,
        "script_status": script_status,
        "console_logs": console_logs,
        "error_messages": error_messages,
        "raw_response": data,
    }
