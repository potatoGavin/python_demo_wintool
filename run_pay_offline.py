import xlwt
import xlrd
import time
import sys
from concurrent.futures import ThreadPoolExecutor

# 执行主方法
def runCompare(file_path,queue):
    queue.put('开始读取excel文件...')

    try:
        workbook = xlrd.open_workbook(file_path)

        excel_saas = []
        excel_off = []

        # 读取excel回调 平台
        def data_excel_read_rollback_saas(future):
            nonlocal excel_saas
            excel_saas = data_excel_read_rollback(future.result())
            queue.put('平台支付数据读取完成')
            pass

        # 读取excel回调 线下系统
        def data_excel_read_rollback_off(future):
            nonlocal excel_off
            excel_off = data_excel_read_rollback(future.result())
            queue.put('线下支付数据读取完成')
            pass

        # 对比结果回调 平台
        def data_compare_rollback_saas(future):
            nonlocal excel_saas
            excel_saas = future.result()
            queue.put('saas平台去比较线下系统 对比完毕')
            pass

        # 对比结果回调 线下系统
        def data_compare_rollback_off(future):
            nonlocal excel_off
            excel_off = future.result()
            queue.put('线下系统去比较saas平台 对比完毕')
            pass

        # 读取excel数据
        with ThreadPoolExecutor(max_workers=2) as pool_read:
            queue.put('开始读取平台支付数据...')
            pool_read.submit(data_excel_read, workbook.sheets()[0], 'ExcelData').add_done_callback(data_excel_read_rollback_saas)
            queue.put('开始读取线下系统支付数据...')
            pool_read.submit(data_excel_read, workbook.sheets()[1], 'ExcelData').add_done_callback(data_excel_read_rollback_off)
            pass

        # 对比 excel 数据
        with ThreadPoolExecutor(max_workers=2) as pool_compare:
            queue.put('开始对比：平台去比较线下系统...')
            pool_compare.submit(data_compare, excel_saas,excel_off, 1).add_done_callback(data_compare_rollback_saas)
            queue.put('开始对比：线下系统去比较saas平台...')
            pool_compare.submit(data_compare, excel_off,excel_saas, 2).add_done_callback(data_compare_rollback_off)
            pass

        queue.put('开始写入结果到excel...')

        # 写入结果到Excel
        data_excel_write((excel_saas+excel_off),queue,file_path)

    except Exception as e:
        queue.put('对比发生异常,请检查文件格式是否正确,异常信息为：%s' % e)
    finally:
        queue.put('对比完成!')
    pass

# 读取Excel信息
def data_excel_read(sheet,objName):
    obj=getattr(sys.modules[__name__], objName)
    return [obj(sheet.row_values(i)) for i in range(sheet.nrows)]

# 读取完Excel回调
def data_excel_read_rollback(listdata):
    return list(filter(lambda item: not item.istitle, listdata))
    pass

# 写入数据到excel
def data_excel_write(listData,queue,file_path):
    result_data=[]
    for item in listData:
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

    mySheet.write(0, 0, "商户单号", style)
    mySheet.write(0, 1, "平台授权码", style)
    mySheet.write(0, 2, "线下授权码", style)
    mySheet.write(0, 3, "线上交易时间", style)
    mySheet.write(0, 4, "线下交易时间", style)
    mySheet.write(0, 5, "线上交易金额", style)
    mySheet.write(0, 6, "线下交易金额", style)
    mySheet.write(0, 7, "线上交易状态", style)
    mySheet.write(0, 8, "线下交易状态", style)
    mySheet.write(0, 9, "异常原因", style)

    if len(listData)==0:
        mySheet.write(1, 0, '没毛病！')
        queue.put('没有异常单,完全没毛病！')
        pass
    else:
        rowIndex = 1
        for item in listData:
            mySheet.write(rowIndex, 0, item.orderno)
            mySheet.write(rowIndex, 1, item.paynosaas)
            mySheet.write(rowIndex, 2, item.paynooff)
            mySheet.write(rowIndex, 3, item.paytimesaas)
            mySheet.write(rowIndex, 4, item.paytiemoff)
            mySheet.write(rowIndex, 5, item.paymoneysaas)
            mySheet.write(rowIndex, 6, item.paymoneyoff)
            mySheet.write(rowIndex, 7, item.statussaas)
            mySheet.write(rowIndex, 8, item.statusoff)
            mySheet.write(rowIndex, 9, item.reason)
            rowIndex += 1
            queue.put("异常单: 订单号: %s \t 异常原因：%s" %(item.orderno,item.reason))
            pass
        pass

    file_dir = file_path[0:file_path.rfind('/', 0)]
    file_name = file_path[file_path.rfind('/', 0):]
    fullPath=file_dir+'/'+('对比结果%s%s' %(int(time.time()),file_name[1:],))
    myWorkbook.save(fullPath)
    queue.put('结果文件地址：'+fullPath)
    pass

# 数据对比
def data_compare(mainlist, comparelist, compare_type):
    time.sleep(0.01)
    temp = list(map(lambda item: compare_main(item, comparelist, compare_type), mainlist))
    return list(filter(lambda i: i != None, temp))
    pass

# 对比主方法
def compare_main(obj_this, datalist, atype):
    result_obj = None
    obj_will_compare = CompareModel()
    templist = list(filter(lambda item: item.orderno == obj_this.orderno, datalist))

    if not templist:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '线下系统无此订单')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, 'SaaS系统无此订单')
            pass
        pass
    else:
        obj_will_compare = templist[0]
        pass

    if obj_this.status != obj_will_compare.status:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '状态不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '状态不一致')
            pass
        pass

    if float(obj_this.money) != float(obj_will_compare.money):
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '订单金额不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '订单金额不一致')
            pass
        pass

    if obj_this.payno != obj_will_compare.payno:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this, obj_will_compare, '支付码不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '支付码不一致')
            pass
        pass

    if obj_this.paytime != obj_will_compare.paytime:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '交易时间不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '交易时间不一致')
            pass
        pass

    return result_obj
    pass

# 生成异常信息实体
def compare_createobj(orderno,obj_saas,obj_off,reason):
    result=CompareResult()

    result.orderno = orderno
    result.paynosaas = obj_saas.payno
    result.paynooff = obj_off.payno
    result.paytimesaas = obj_saas.paytime
    result.paytiemoff = obj_off.paytime
    result.paymoneysaas = obj_saas.money
    result.paymoneyoff = obj_off.money
    result.statussaas = obj_saas.status
    result.statusoff = obj_off.status
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
        return '{}-{}-{}'.format(t_year, t_month, t_day, )
        pass
    except:
        return ''
    pass

# excel信息实体
class ExcelData:
    def __init__(self, rowInfo):
        self.istitle = False
        self.orderno = rowInfo[0]
        self.payno = rowInfo[1]
        self.paytime = formate_time(rowInfo[2])
        self.money = rowInfo[3]
        self.status = rowInfo[4]

        if not self.orderno[0:3].isdecimal():
            self.istitle = True
            pass
        pass
    def __str__(self):
        return '订单号：{},支付授权码：{}'.format(self.orderno,self.payno)

class CompareModel():
    def __init__(self):
        self.orderno = ''
        self.payno = ''
        self.paytime = ''
        self.money = ''
        self.status = ''
        pass

# 对比结果
class CompareResult:
    def __init__(self):
        self.orderno=''
        self.paynosaas=''
        self.paynooff = ''
        self.paytimesaas=''
        self.paytiemoff=''
        self.paymoneysaas = ''
        self.paymoneyoff = ''
        self.statussaas=''
        self.statusoff=''
        self.reason=''
        pass

    def __str__(self):
        return '订单号：{}，授权码：{},{}'.format(self.orderno,self.paymoneyoff,self.paymoneysaas)

