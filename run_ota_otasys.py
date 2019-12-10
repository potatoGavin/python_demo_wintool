import xlwt
import xlrd
import time
import sys
from concurrent.futures import ThreadPoolExecutor

# 对比主方法
def run_compare(file_path,queue):
    queue.put('开始读取excel文件...')

    try:
        # 获取excel对象
        workbook = xlrd.open_workbook(file_path)

        excel_saas = []
        excel_ota = []

        # 读取excel回调 平台
        def data_excel_read_rollback_saas(future):
            nonlocal excel_saas
            excel_saas = data_excel_read_rollback(future.result())
            queue.put('平台支付数据读取完成')
            pass

        # 读取excel回调 OTA系统
        def data_excel_read_rollback_ota(future):
            nonlocal excel_ota
            excel_ota = data_excel_read_rollback(future.result())
            pass

        # 对比结果回调 SaaS平台
        def data_compare_rollback_saas(future):
            nonlocal excel_saas
            excel_saas = future.result()
            queue.put('saas平台去比较OTA系统 对比完毕')
            pass

        # 对比结果回调 OTA系统
        def data_compare_rollback_ota(future):
            nonlocal excel_ota
            excel_ota = future.result()
            queue.put('OTA系统去比较saas平台 对比完毕')
            pass

        # 读取excel数据
        with ThreadPoolExecutor(max_workers=2) as pool_read:
            queue.put('开始读取平台订单数据...')
            pool_read.submit(data_excel_read, workbook.sheets()[0], 'ExcelData').add_done_callback(data_excel_read_rollback_saas)
            queue.put('开始读取OTA订单数据...')
            pool_read.submit(data_excel_read, workbook.sheets()[1], 'ExcelData').add_done_callback(data_excel_read_rollback_ota)
            pass

        # 对比 excel 数据
        with ThreadPoolExecutor(max_workers=2) as pool_compare:
            queue.put('开始对比：平台去比较线下系统...')
            pool_compare.submit(data_compare, excel_saas,excel_ota, 1).add_done_callback(data_compare_rollback_saas)
            queue.put('开始对比：线下系统去比较saas平台...')
            pool_compare.submit(data_compare, excel_ota,excel_saas, 2).add_done_callback(data_compare_rollback_ota)
            pass

        queue.put('开始写入结果到excel...')

        # 写入结果到Excel
        data_excel_write((excel_saas + excel_ota), queue, file_path)
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

# 写入数据
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
    mySheet.write(0, 1, "购买数量-平台", style)
    mySheet.write(0, 2, "购买数量-OTA", style)
    mySheet.write(0, 3, "核销数量-平台", style)
    mySheet.write(0, 4, "核销数量-OTA", style)
    mySheet.write(0, 5, "退款数量-平台", style)
    mySheet.write(0, 6, "退款数量-平台", style)
    mySheet.write(0, 7, "对比时间-线上", style)
    mySheet.write(0, 8, "对比时间-OTA", style)
    mySheet.write(0, 9, "异常原因", style)

    if len(listData)==0:
        mySheet.write(1, 0, '没毛病！')
        queue.put('没有异常单,完全没毛病！')
        pass
    else:
        rowIndex = 1
        for item in listData:
            mySheet.write(rowIndex, 0, item.orderno)
            mySheet.write(rowIndex, 1, item.buycnt)
            mySheet.write(rowIndex, 2, item.buycntota)
            mySheet.write(rowIndex, 3, item.checkcnt)
            mySheet.write(rowIndex, 4, item.checkcntota)
            mySheet.write(rowIndex, 5, item.refundcnt)
            mySheet.write(rowIndex, 6, item.refundcntota)
            mySheet.write(rowIndex, 7, item.comtime)
            mySheet.write(rowIndex, 8, item.comtimeota)
            mySheet.write(rowIndex, 9, item.reason)
            rowIndex += 1
            queue.put("异常单: 订单号: %s \t 异常原因：%s" %(item.orderno,item.reason))
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
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, 'OTA系统无此订单')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, 'SaaS系统无此订单')
            pass
        pass
    else:
        obj_will_compare = templist[0]
        pass

    # 订单数量
    if obj_this.buycnt != obj_will_compare.buycnt:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '订单数量不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '订单数量不一致')
            pass
        pass

    # 核销数量
    if float(obj_this.checkcnt) != float(obj_will_compare.checkcnt):
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '核销数量不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '核销数量不一致')
            pass
        pass

    # 退票数量
    if obj_this.refundcnt != obj_will_compare.refundcnt:
        if atype == 1:
            return compare_createobj(obj_this.orderno, obj_this, obj_will_compare, '退票数量不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '退票数量不一致')
            pass
        pass

    # 对比时间
    if obj_this.comtime != obj_will_compare.comtime:
        if atype == 1:
            return compare_createobj(obj_this.orderno,obj_this, obj_will_compare, '对比时间不一致')
            pass
        else:
            return compare_createobj(obj_this.orderno,obj_will_compare, obj_this, '对比时间不一致')
            pass
        pass

    return result_obj
    pass

# 生成异常信息实体
def compare_createobj(orderno,objsaas,objota,msg):
    temp = CompareResult()

    temp.orderno = orderno
    temp.buycnt = objsaas.buycnt
    temp.buycntota = objota.buycnt
    temp.checkcnt = objsaas.checkcnt
    temp.checkcntota = objota.checkcnt
    temp.refundcnt = objsaas.refundcnt
    temp.refundcntota = objota.refundcnt
    temp.comtime = objsaas.comtime
    temp.comtimeota = objota.comtime
    temp.reason = msg

    return temp
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
        t_month = value[5:7]
        t_day = value[8:10]
        return '{}-{}-{}'.format(t_year, t_month, t_day)
        pass
    except:
        return ''
    pass

# 数据实体
class ExcelData:
    def __init__(self, rowInfo):
        self.istitle = False
        self.orderno = rowInfo[0]
        self.buycnt = int(rowInfo[1]) if isinstance(rowInfo[1],(int,float)) else 0
        self.checkcnt = int(rowInfo[2]) if isinstance(rowInfo[2],(int,float)) else 0
        self.refundcnt = int(rowInfo[3]) if isinstance(rowInfo[3],(int,float)) else 0
        self.comtime =formate_time(rowInfo[4])
        if not self.orderno[0:3].isdecimal():
            self.istitle = True
            pass
        pass

        pass

    def __str__(self):
        return '订单号:{0},购买数量：{1}，核销数量：{2}，退款数量：{3}，时间：{4}'.format(self.orderno,self.buycnt,self.checkcnt,self.refundcnt,self.comtime)

    pass

# 可对比实体
class CompareModel():
    def __init__(self):
        self.orderno = ''
        self.buycnt = ''
        self.checkcnt = ''
        self.refundcnt = ''
        self.comtime = ''
        pass
    pass

# 对比结果
class CompareResult:
    def __init__(self):
        self.orderno=''
        self.buycnt = ''
        self.buycntota = ''
        self.checkcnt = ''
        self.checkcntota = ''
        self.refundcnt = ''
        self.refundcntota = ''
        self.comtime = ''
        self.comtimeota = ''
        self.reason=''
        pass