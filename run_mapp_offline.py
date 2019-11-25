import xlwt
import xlrd
import time
import sys
from concurrent.futures import ThreadPoolExecutor


def run_compare(file_path,queue):
    """
    对比主方法
    :param file_path: 要对比的文件地址
    :param queue: 读取消息的队列
    :return: 对比结果
    """
    queue.put('开始读取excel文件...')

    try:
        workbook = xlrd.open_workbook(file_path)

        excel_saas = []
        excel_off = []


        def data_excel_read_rollback_saas(future):
            """
            读取excel回调 平台
            :param future: 回调参数
            :return: 读取到的平台数据到 excel_saas
            """
            nonlocal excel_saas
            excel_saas = data_excel_read_rollback(future.result())
            queue.put('平台小程序数据读取完成')
            pass

        # 读取excel回调 线下系统
        def data_excel_read_rollback_off(future):
            nonlocal excel_off
            excel_off = data_excel_read_rollback(future.result())
            queue.put('线下小程序数据读取完成')
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
            queue.put('开始读取平台小程序数据...')
            pool_read.submit(data_excel_read, workbook.sheets()[0], 'ExcelData').add_done_callback(data_excel_read_rollback_saas)
            queue.put('开始读取线下系统小程序数据...')
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
        data_excel_write((excel_saas + excel_off), queue, file_path)

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

#写入数据
def data_excel_write(listData,queue,file_path):

    result_data = []
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

    mySheet.write(0, 0, "订单号", style)
    mySheet.write(0, 1, "产品ID-线上", style)
    mySheet.write(0, 2, "产品ID-线下", style)
    mySheet.write(0, 3, "线下编码-线上", style)
    mySheet.write(0, 4, "线下编码-线下", style)
    mySheet.write(0, 5, "结算单价-线上", style)
    mySheet.write(0, 6, "结算单价-线下", style)
    mySheet.write(0, 7, "订单数量-线上", style)
    mySheet.write(0, 8, "订单数量-线下", style)
    mySheet.write(0, 9, "核销数量-线上", style)
    mySheet.write(0, 10, "核销数量-线下", style)
    mySheet.write(0, 11, "退款数量-线上", style)
    mySheet.write(0, 12, "退款数量-线下", style)
    mySheet.write(0, 13, "下单时间-线上", style)
    mySheet.write(0, 14, "下单时间-线下", style)
    mySheet.write(0, 15, "退款时间-线上", style)
    mySheet.write(0, 16, "退款时间-线下", style)
    mySheet.write(0, 17, "核销时间-线上", style)
    mySheet.write(0, 18, "核销时间-线下", style)
    mySheet.write(0, 19, "订单状态-线上", style)
    mySheet.write(0, 20, "订单状态-线下", style)
    mySheet.write(0, 21, "异常原因", style)

    if len(listData)==0:
        mySheet.write(1, 0, '没毛病！')
        queue.put('没有异常单,完全没毛病！')
        pass
    else:
        rowIndex = 1
        for item in listData:
            mySheet.write(rowIndex, 0, item.orderno)
            mySheet.write(rowIndex, 1, item.productid)
            mySheet.write(rowIndex, 2, item.productidzj)
            mySheet.write(rowIndex, 3, item.productoff)
            mySheet.write(rowIndex, 4, item.productoffzj)
            mySheet.write(rowIndex, 5, item.settlefee)
            mySheet.write(rowIndex, 6, item.settlefeezj)
            mySheet.write(rowIndex, 7, item.ordercnt)
            mySheet.write(rowIndex, 8, item.ordercntzj)
            mySheet.write(rowIndex, 9, item.checkcnt)
            mySheet.write(rowIndex, 10, item.checkcntzj)
            mySheet.write(rowIndex, 11, item.refundcnt)
            mySheet.write(rowIndex, 12, item.refundcntzj)
            mySheet.write(rowIndex, 13, item.createtime)
            mySheet.write(rowIndex, 14, item.createtimezj)
            mySheet.write(rowIndex, 15, item.refundtime)
            mySheet.write(rowIndex, 16, item.refundtimezj)
            mySheet.write(rowIndex, 17, item.checktime)
            mySheet.write(rowIndex, 18, item.checktimezj)
            mySheet.write(rowIndex, 19, item.status)
            mySheet.write(rowIndex, 20, item.statuszj)
            mySheet.write(rowIndex, 21, item.reason)
            rowIndex += 1
            queue.put("异常单: 订单号: %s \t 异常原因：%s" % (item.orderno, item.reason))
            pass
        pass

    file_dir = file_path[0:file_path.rfind('/', 0)]
    file_name = file_path[file_path.rfind('/', 0):]
    fullPath = file_dir + '/' + ('对比结果%s%s' % (int(time.time()), file_name[1:],))
    myWorkbook.save(fullPath)
    queue.put('结果文件地址：' + fullPath)
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

    # 线上产品ID
    if obj_this.productid != obj_will_compare.productid:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '线上产品ID不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '线上产品ID不一致')
            pass
        pass

    # 线下产品ID
    if obj_this.productoff != obj_will_compare.productoff:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '线下产品编码不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '线下产品编码不一致')
            pass
        pass

    # 结算价
    if float(obj_this.settlefee) != float(obj_will_compare.settlefee):
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '结算单价不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '结算单价不一致')
            pass
        pass

    # 订单数量
    if obj_this.ordercnt != obj_will_compare.ordercnt:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '订单数量不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '订单数量不一致')
            pass
        pass

    # 核销数量
    if obj_this.checkcnt != obj_will_compare.checkcnt:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '核销数量不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '核销数量不一致')
            pass
        pass

    # 退款数量
    if obj_this.refundcnt != obj_will_compare.refundcnt:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '退款数量不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '退款数量不一致')
            pass
        pass

    # 订单状态
    if obj_this.status != obj_will_compare.status:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this, obj_will_compare, '订单状态不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '订单状态不一致')
            pass
        pass

    # 下单时间
    if obj_this.createtime != obj_will_compare.createtime:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this, obj_will_compare, '下单时间不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '下单时间不一致')
            pass
        pass

    # 退款时间
    if obj_this.refundtime != obj_will_compare.refundtime:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this, obj_will_compare, '退款时间不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '退款时间不一致')
            pass
        pass

    # 核销时间
    if obj_this.checktime != obj_will_compare.checktime:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this, obj_will_compare, '核销时间不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '核销时间不一致')
            pass
        pass

    return result_obj
    pass

# 生成异常信息实体
def compare_createobj(orderno,obj_saas,obj_off,reason):
    result=CompareResult()

    result.orderno = orderno
    result.productid = obj_saas.productid
    result.productidzj =  obj_off.productid
    result.productoff = obj_saas.productoff
    result.productoffzj = obj_off.productoff
    result.settlefee = obj_saas.settlefee
    result.settlefeezj = obj_off.settlefee
    result.ordercnt = obj_saas.ordercnt
    result.ordercntzj = obj_off.ordercnt
    result.checkcnt = obj_saas.checkcnt
    result.checkcntzj = obj_off.checkcnt
    result.refundcnt = obj_saas.refundcnt
    result.refundcntzj = obj_off.refundcnt
    result.createtime = obj_saas.createtime
    result.createtimezj = obj_off.createtime
    result.refundtime = obj_saas.refundtime
    result.refundtimezj = obj_off.refundtime
    result.checktime = obj_saas.checktime
    result.checktimezj = obj_off.checktime
    result.status = obj_saas.status
    result.statuszj = obj_off.status
    result.reason = reason
    return result
    pass

# 时间统一格式化
def formate_time(value):
    """ 时间格式统一为 yyyy-MM-dd

    :param value:从excel读取的时间字符串
    :return 格式化的字符串,如果输入为空则返回空
    :rtype str

    """
    try:
        if not value:
            return ''
        if value.find('-')>0:
            temp = time.strptime(value, '%Y-%m-%d %H:%M:%S')
            return time.strftime('%Y-%m-%d', temp)
        else:
            temp=time.strptime(value,'%Y/%m/%d %H:%M:%S')
            return time.strftime('%Y-%m-%d',temp)
        pass
    except:
        return ''
    pass

# Excel数据实体
class ExcelData:
    def __init__(self, rowInfo):
        self.istitle = False
        self.orderno = rowInfo[0]
        self.productid = rowInfo[1]
        self.productoff = rowInfo[2]
        self.settlefee = rowInfo[3]
        self.ordercnt = rowInfo[4]
        self.checkcnt = rowInfo[5]
        self.refundcnt = rowInfo[6]
        self.createtime =formate_time(rowInfo[7])
        self.refundtime =formate_time(rowInfo[8])
        self.checktime =formate_time(rowInfo[9])
        self.status = rowInfo[10]

        if not self.orderno[0:3].isdecimal():
            self.istitle = True
            pass
        pass
    pass

# 可对比实体
class CompareModel():
    def __init__(self):
        self.orderno = ''
        self.productid = ''
        self.productoff = ''
        self.settlefee = ''
        self.ordercnt = ''
        self.checkcnt = ''
        self.refundcnt = ''
        self.createtime = ''
        self.refundtime = ''
        self.checktime = ''
        self.status = ''
        pass
    pass

# 对比结果
class CompareResult:
    def __init__(self):
        self.orderno=''
        self.productid=''
        self.productidzj=''
        self.productoff = ''
        self.productoffzj = ''
        self.settlefee = ''
        self.settlefeezj = ''
        self.ordercnt = ''
        self.ordercntzj = ''
        self.checkcnt = ''
        self.checkcntzj = ''
        self.refundcnt = ''
        self.refundcntzj = ''
        self.createtime = ''
        self.createtimezj = ''
        self.refundtime = ''
        self.refundtimezj = ''
        self.checktime = ''
        self.checktimezj = ''
        self.status = ''
        self.statuszj = ''
        self.reason=''
        pass
