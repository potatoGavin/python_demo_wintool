import sys
import xlwt
import xlrd
import time
from concurrent.futures import ThreadPoolExecutor


def run_compare(file_path, queue):
    """
    执行主方法
    :param file_path: 文件地址
    :param queue: 队列对象
    :return:
    """
    queue.put('开始读取excel文件...')

    try:
        workbook = xlrd.open_workbook(file_path)
        excel_saas = []
        excel_pay = []

        # 读取excel回调 平台
        def data_excel_read_rollback_saas(future):
            nonlocal excel_saas
            excel_saas= data_excel_read_rollback(future.result())
            queue.put('平台支付数据读取完成')
            pass

        # 读取excel回调 第三方
        def data_excel_read_rollback_pay(future):
            nonlocal excel_pay
            excel_pay = data_excel_read_rollback(future.result())
            queue.put('第三方支付数据读取完成')
            pass

        # 解析excel回调 平台
        def data_excel_analysis_rollback_saas(future):
            nonlocal excel_saas
            excel_saas=future.result()
            queue.put('平台数据解析完毕')
            pass

        # 解析excel回调 第三方
        def data_excel_analysis_rollback_pay(future):
            nonlocal excel_pay
            excel_pay=future.result()
            queue.put('第三方数据解析完毕')
            pass

        # 对比结果回调 平台
        def data_compare_rollback_saas(future):
            nonlocal excel_saas
            excel_saas = future.result()
            queue.put('平台去比较支付平台 对比完毕')
            pass

        # 对比结果回调 支付平台
        def data_compare_rollback_pay(future):
            nonlocal excel_pay
            excel_pay=future.result()
            queue.put('支付平台去比较saas平台 对比完毕')
            pass

        # 读取excel数据
        with ThreadPoolExecutor(max_workers=2) as pool_read:
            queue.put('开始读取平台支付数据...')
            pool_read.submit(data_excel_read, workbook.sheets()[0], 'ExcelSaas').add_done_callback(data_excel_read_rollback_saas)
            queue.put('开始读取第三方支付数据...')
            pool_read.submit(data_excel_read, workbook.sheets()[1], 'ExcelWechate').add_done_callback(data_excel_read_rollback_pay)
            pass

        # 解析 excel 数据
        with ThreadPoolExecutor(max_workers=2) as pool_analysis:
            queue.put('开始解析平台支付数据...')
            pool_analysis.submit(data_excel_analysis,excel_saas,1).add_done_callback(data_excel_analysis_rollback_saas)
            queue.put('开始解析第三方支付数据...')
            pool_analysis.submit(data_excel_analysis, excel_pay,2).add_done_callback(data_excel_analysis_rollback_pay)
            pass

        # 对比 excel 数据
        with ThreadPoolExecutor(max_workers=2) as pool_compare:
            queue.put('开始对比：平台去比较支付平台...')
            pool_compare.submit(data_compare, excel_saas,excel_pay, 1).add_done_callback(data_compare_rollback_saas)
            queue.put('开始对比：支付平台去比较saas平台...')
            pool_compare.submit(data_compare, excel_pay,excel_saas, 2).add_done_callback(data_compare_rollback_pay)
            pass

        queue.put('开始写入结果到excel...')

        # 写入结果到Excel
        data_excel_write(excel_saas+excel_pay,queue,file_path)

    except Exception as e:
        queue.put('对比发生异常,请检查文件格式是否正确,异常信息为：%s' %e)
    finally:
        queue.put('对比完成!')

    pass


def data_compare(main_list, compare_list, compare_type):
    """
    数据对比方法
    :param main_list: 主比对文件
    :param compare_list: 去对比的文件
    :param compare_type: 对比类型
    :return:
    """
    temp = list(map(lambda item: compare_main(item, list(compare_list), compare_type), list(main_list)))
    return list(filter(lambda item: item != None, temp))
    pass


def data_excel_read(sheet, obj_name):
    """
    读取Excel信息
    :param sheet: 要读取的excel表单
    :param obj_name: 赋值的对象名称
    :return:
    """
    obj = getattr(sys.modules[__name__], obj_name)
    return [obj(sheet.row_values(i)) for i in range(sheet.nrows)]


def data_excel_read_rollback(list_data):
    """
    读取完Excel回调
    :param list_data: 读取到的数据列表
    :return:
    """
    return list(filter(lambda item: not item.istitle, list_data))
    pass


def data_excel_analysis(list_data, list_type):
    """
    解析Excel的列表信息
    :param list_data: 列表数据
    :param list_type: 解析类型
    :return:
    """
    result_list = []
    for item in list_data:
        templist = list(filter(lambda temp: temp.orderno == item.orderno, list_data))

        if len(templist) <= 1:
            result_list.append(data_formate_detail_saas(item) if list_type==1 else data_formate_detail_pay(item))
            pass
        else:

            result_temp=map(lambda temp:(data_formate_detail_saas(temp) if list_type==1 else data_formate_detail_pay(temp)),templist)
            result_temp = list(result_temp)

            obj_pay=list(filter(lambda k:k.status=='pay',result_temp))[0]
            obj_refund = list(filter(lambda k:k.status=='refund',result_temp))[0]
            obj_result=DataDetail()
            obj_result.orderno=obj_pay.orderno
            obj_result.payno = obj_pay.payno
            obj_result.paytime = obj_pay.paytime
            obj_result.payamount = obj_pay.payamount
            obj_result.refundtime = obj_refund.refundtime
            obj_result.refundamount = obj_refund.refundamount
            obj_result.status = 'refund'

            result_list.append(obj_result)
            pass
        pass

    return list(filter(lambda item: item != None, result_list))
    pass


def data_excel_write(list_data,queue,file_path):
    """
    写入数据到excel
    :param list_data:
    :param queue:
    :param file_path:
    :return:
    """
    result_data=[]
    for item in list_data:
        temp = list(filter(lambda x: x.orderno == item.orderno, result_data))
        if len(temp) == 0:
            result_data.append(item)
        pass

    # 创建Excel工作薄,编码方式为：utf-8
    myWorkbook = xlwt.Workbook(encoding='utf-8')
    # 添加Excel工作表
    mySheet = myWorkbook.add_sheet('对比结果', cell_overwrite_ok=True)

    # 使用样式
    style = xlwt.XFStyle()  # 创建一个样式
    # 为样式创建字体
    font = xlwt.Font()
    font.name = '宋体'
    font.bold = True  # 是否加粗，默认值为false不加粗
    style.font = font

    # 设置对齐方式，文本的对齐方式
    alignment = xlwt.Alignment()
    alignment.horz = xlwt.Alignment.HORZ_CENTER  # 水平居中
    alignment.vert = xlwt.Alignment.VERT_CENTER  # 垂直居中
    style.alignment = alignment

    mySheet.write(0, 0, "订单号", style)
    mySheet.write(0, 1, "支付单号", style)
    mySheet.write(0, 2, "平台支付时间", style)
    mySheet.write(0, 3, "第三方支付时间", style)
    mySheet.write(0, 4, "平台支付金额", style)
    mySheet.write(0, 5, "第三方支付金额", style)
    mySheet.write(0, 6, "平台退款时间", style)
    mySheet.write(0, 7, "第三方退款时间", style)
    mySheet.write(0, 8, "平台退款金额", style)
    mySheet.write(0, 9, "第三方退款金额", style)
    mySheet.write(0, 10, "平台状态", style)
    mySheet.write(0, 11, "第三方状态", style)
    mySheet.write(0, 12, "异常原因", style)

    rowIndex = 1
    if len(result_data)==0:
        queue.put("没有异常单，完全没毛病！")
        mySheet.write(1, 0, '没毛病！')
        pass
    else:
        for item in result_data:
            mySheet.write(rowIndex, 0, item.orderno)
            mySheet.write(rowIndex, 1, item.payno)
            mySheet.write(rowIndex, 2, item.paytimesaas)
            mySheet.write(rowIndex, 3, item.paytiempay)
            mySheet.write(rowIndex, 4, item.paymoneysaas)
            mySheet.write(rowIndex, 5, item.paymoneypay)
            mySheet.write(rowIndex, 6, item.refundtimesaas)
            mySheet.write(rowIndex, 7, item.refundtimepay)
            mySheet.write(rowIndex, 8, item.refundmoneysaas)
            mySheet.write(rowIndex, 9, item.refundmoneypay)
            mySheet.write(rowIndex, 10, item.statussaas)
            mySheet.write(rowIndex, 11,item.statuspay)
            mySheet.write(rowIndex, 12, item.reason)
            rowIndex += 1
            queue.put("异常单: 流水号:%s \t 订单号：%s \t 异常原因：%s" %(item.orderno,item.payno,item.reason))

    file_dir = file_path[0:file_path.rfind('/', 0)]
    file_name = file_path[file_path.rfind('/', 0):]
    fullPath=file_dir+'/'+('对比结果%s%s' %(int(time.time()),file_name[1:],))
    myWorkbook.save(fullPath)
    queue.put('结果文件地址：'+fullPath)
    pass


def data_formate_detail_saas(obj):
    """
    执行对比的实体格式化 平台
    :param obj:对象
    :return:
    """
    resultObj = DataDetail()
    resultObj.orderno = obj.orderno
    resultObj.payno = obj.payno
    if float(obj.money) < 0:
        resultObj.paytime = ''
        resultObj.payamount = 0
        resultObj.refundtime = obj.paytime
        resultObj.refundamount = abs(float(obj.money))
        pass
    else:
        resultObj.paytime = obj.paytime
        resultObj.payamount =  abs(float(obj.money))
        resultObj.refundtime = ''
        resultObj.refundamount = 0
        pass

    resultObj.status = obj.status
    return resultObj

# 执行对比的实体格式化 第三方
def data_formate_detail_pay(obj):
    resultObj = DataDetail()
    resultObj.orderno = obj.orderno
    resultObj.payno = obj.payno

    if obj.status == 'SUCCESS':
        resultObj.paytime = obj.paytime
        resultObj.payamount = abs(float(obj.paymoney))
        resultObj.refundtime = ''
        resultObj.refundamount = 0
        resultObj.status = 'pay'
    else:
        resultObj.paytime = ''
        resultObj.payamount = 0
        resultObj.refundtime = obj.paytime
        resultObj.refundamount = abs(float(obj.refundmoney))
        resultObj.status = 'refund'
        pass

    return resultObj
    pass

# 对比主方法
def compare_main(obj_this, datalist, atype):
    result_obj = None
    obj_will_compare=DataDetail()
    templist=list(filter(lambda item:item.orderno==obj_this.orderno,datalist))

    if not templist:
        if atype==1:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_this, obj_will_compare, '支付系统无此订单')
            pass
        else:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_will_compare,obj_this,'SaaS系统无此订单')
            pass
        pass
    else:
        obj_will_compare = templist[0]
        pass


    if obj_this.status != obj_will_compare.status:
        if atype==1:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_this, obj_will_compare, '状态不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_will_compare,obj_this,'状态不一致')
            pass
        pass

    if obj_this.payamount != obj_will_compare.payamount:
        if atype==1:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_this, obj_will_compare, '支付金额不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_will_compare,obj_this,'支付金额不一致')
            pass
        pass

    if obj_this.refundamount != obj_will_compare.refundamount:
        if atype==1:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_this, obj_will_compare, '退款金额不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_will_compare,obj_this,'退款金额不一致')
            pass
        pass

    if obj_this.paytime != obj_will_compare.paytime:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_this, obj_will_compare, '支付时间不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_will_compare,obj_this,'支付时间不一致')
            pass
        pass

    if obj_this.refundtime != obj_will_compare.refundtime:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_this, obj_will_compare, '退款时间不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno, obj_this.payno, obj_will_compare,obj_this,'退款时间不一致')
            pass
        pass

    return result_obj

    pass

# 生成异常信息实体
def compare_createobj(orderno,payno,obj_saas,obj_pay,reason):
    result=DataResult()

    result.orderno = orderno
    result.payno = payno
    result.paytimesaas = obj_saas.paytime
    result.paytiempay = obj_pay.paytime
    result.paymoneysaas = obj_saas.payamount
    result.paymoneypay = obj_pay.payamount
    result.refundtimesaas = obj_saas.refundtime
    result.refundtimepay = obj_pay.refundtime
    result.refundmoneysaas = obj_saas.refundamount
    result.refundmoneypay = obj_pay.refundamount
    result.statussaas = obj_saas.status
    result.statuspay = obj_pay.status
    result.reason = reason

    return result
    pass


def formate_time(value):
    """ 时间统一格式化 yyyy-MM-dd
    :param value:从excel读取的时间字符串
    :return 格式化的字符串,如果输入为空则返回空
    :rtype str

    """
    try:
        if not value:
            return ''
        t_year = value[0:4]
        t_month = value[5:2]
        t_day = value[8:2]
        return '{}-{}-{}'.format(t_year, t_month, t_day)
        pass
    except:
        return ''
    pass

# 平台数据实体
class ExcelSaas(object):
    def __init__(self, rowInfo):
        self.istitle = False
        self.orderno = rowInfo[0]
        self.payno = rowInfo[1]
        self.paytime = formate_time(rowInfo[2])
        self.money = rowInfo[3]
        self.status = rowInfo[4]

        if not self.orderno[0:3].isdecimal():
            self.istitle=True
            pass
        pass

    def __str__(self):
        return '平台交易流水号：{},订单号：{}'.format(self.orderno,self.payno)

# 微信数据实体
class ExcelWechate(object):
    def __init__(self, rowInfo):
        self.istitle = False
        self.orderno = rowInfo[0].replace('`','').strip()
        self.payno = rowInfo[1].replace('`','').strip()
        self.paytime = formate_time(rowInfo[2].replace('`', '').strip())
        self.paymoney = rowInfo[3].replace('`','').strip()
        self.refundmoney = rowInfo[4].replace('`','').strip()
        self.status = rowInfo[5].replace('`','').strip()

        if not self.orderno[0:1].isdecimal():
            self.istitle=True
            pass
        pass

    def __str__(self):
        return '微信流水号：{}，订单号：{}'.format(self.orderno,self.payno)

# 可对比实体
class DataDetail(object):
      def __init__(self):
          self.orderno = ''
          self.payno = ''
          self.paytime = ''
          self.payamount = ''
          self.refundtime = ''
          self.refundamount = ''
          self.status = ''
          pass

      def __str__(self):
          return '交易流水号：{},订单号：{}'.format(self.orderno,self.payno)

# 结果实体
class DataResult(object):
    def __init__(self):
        self.orderno=''
        self.payno=''
        self.paytimesaas=''
        self.paytiempay=''
        self.paymoneysaas=''
        self.paymoneypay=''
        self.refundtimesaas=''
        self.refundtimepay = ''
        self.refundmoneysaas = ''
        self.refundmoneypay = ''
        self.statussaas=''
        self.statuspay=''
        self.reason=''
        pass

    def __str__(self):
        return "流水号：{}，订单号：{}，原因：{}".format(self.orderno,self.payno,self.reason)

