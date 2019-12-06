from tkinter import *
import tkinter as tk
import tkinter.filedialog
import tkinter.messagebox
from tkinter import ttk
import threading
from queue import Queue

import run_pay_paysys
import run_pay_offline
import run_mapp_offline
import run_ota_otasys

# 支持的所有模式
COMPARE_MODULE = [
    {'identify':'pay_paysys','show':'支付(线上和第三方)'},
    {'identify':'pay_offsys','show':'支付(线上和线下)'},
    {'identify':'mini_wechate','show':'小程序(线上和微信)'},
    {'identify':'mini_offsys','show':'小程序(线上和线下系统)'},
    {'identify':'mini_saas','show':'小程序(线上自营和线上订单明细)'},
    {'identify':'ota_otasys','show':'OTA(线上和OTA系统)'}
]


def func_nopass():
    """
    对没有实现的功能进行友好提示
    :return:None
    """
    tkinter.messagebox.showinfo(title='抱歉', message='当前功能未开放,实在抱歉^-^')
    pass


class App(tk.Tk):
    """
    窗体执行类,所有控件在这里声明
    """
    def __init__(self):
        tk.Tk.__init__(self)
        self.queue = Queue()
        self.module_list = COMPARE_MODULE
        self.compare_file = '',
        self.compare_module = 'pay_paysys'
        self.module_init()
        self.file_init()
        self.lstbox_init()
        pass

    def check_queue(self):
        """
        读取队列中正在执行的方法返回的信息,并打印在listbox中
        :return:None
        """
        while self.queue.qsize():
            try:
                msg = self.queue.get(0)
                self.listbox.insert('end', msg)
            except Queue.Empty:
                pass
            pass
        pass

    def start_que(self):
        self.check_queue()
        if self.thread.is_alive() or self.queue.qsize():
            self.after(100, self.start_que)
        else:
            self.button.config(state="active")
        pass

    def modeul_change(self):
        """
        获取单选按钮的值
        :return: None
        """
        self.compare_module = str(self.radioValue.get())
        pass

    def module_init(self):
        """
        初始化对比模式相关控件-->创建单选按钮
        :return: None
        """
        # 创建Labelframe容器
        group = ttk.Labelframe(self.master, text='请选择对比模式', padding=20, height=14)
        group.pack(fill=X, expand=NO, padx=10, pady=10)
        self.radioValue = StringVar()
        self.radioValue.set('pay_paysys')

        # 使用循环创建多个Radiobutton，并放入Labelframe中
        for i in range(0, 3):
            Radiobutton(group, text=self.module_list[i]["show"], value=self.module_list[i]["identify"], variable=self.radioValue, command=self.modeul_change).\
                grid(row=0,column=i,sticky=W)
        for i in range(3, 6):
            Radiobutton(group, text=self.module_list[i]["show"], value=self.module_list[i]["identify"], variable=self.radioValue, command=self.modeul_change).\
                grid(row=1,column=i - 3,sticky=W)
            pass
        pass

    def file_init(self):
        """
        初始化选择文件和操作按钮控件
        :return:None
        """
        # 创建Panedwindow组件 horizontal
        pwindow = ttk.Panedwindow(self,orient=HORIZONTAL)
        pwindow.pack(fill=X, expand=NO, padx=10, pady=10)

        filePath = StringVar()
        fileInput = ttk.Entry(self, width=80, textvariable=filePath)
        fileInput.pack(fill=None, expand=NO, ipady=5, side=LEFT, pady=5, padx=2)
        pwindow.add(fileInput) # 调用add方法添加组件，每个组件一个区域

        def choose_fiel():
            selectFileName = tk.filedialog.askopenfilename(title='选择文件')  # 选择文件
            filePath.set(selectFileName)
            self.compare_file= selectFileName
            pass

        btn_getfile = Button(self, text='选择文件', font=("仿宋", 18, "bold"), width=10, height=1, command=choose_fiel)
        btn_getfile.pack(fill=None, expand=NO, ipady=2, side=LEFT, pady=5, padx=2)
        pwindow.add(btn_getfile)

        btn_getfile = Button(self, text='开始对比', font=("仿宋", 18, "bold"), width=10, height=1, command=self.run_Compare)
        btn_getfile.pack(fill=None, expand=NO, ipady=2, side=LEFT, pady=5, padx=2)
        self.button=btn_getfile
        pwindow.add(btn_getfile)
        pass

    def lstbox_init(self):
        """
        初始化数据显示用的listbox
        :return: None
        """
        self.listbox = tk.Listbox(self, width=100, height=5)
        yscrollbar = ttk.Scrollbar(self.listbox, command=self.listbox.yview)
        yscrollbar.pack(side=RIGHT, fill=Y)
        self.listbox.config(yscrollcommand=yscrollbar.set)
        self.listbox.pack(fill=BOTH, expand=1, padx=10, pady=10)
        pass

    def run_Compare(self):
        """
        对比按钮单击事件:对操作参数进行判断,并调用对应的方法
        :return:
        """

        self.listbox.delete(0, self.listbox.size())
        self.button.config(state="disabled")

        if len(self.compare_module) <= 0:
            tkinter.messagebox.showinfo(title='警告', message='请选择对比模式')
            self.button.config(state="active")
            return

        if type(self.compare_file) is tuple or (len(self.compare_file) <= 0):
            tkinter.messagebox.showinfo(title='警告', message='请选择对比文件')
            self.button.config(state="active")
            return

        self.thread = ThreadedClient(self.queue, self.compare_file,self.compare_module)
        self.thread.start()
        self.start_que()
        pass


class ThreadedClient(threading.Thread):
    """
    线程执行类
    """
    def __init__(self, queue, compare_file, compare_module):
        """
        类的构造方法
        :param queue:窗体主程序的队列对象
        :param compare_file:选中的文件地址
        :param compare_module:要对比的模式
        """
        threading.Thread.__init__(self)
        self.queue = queue
        self.compare_file = compare_file
        self.compare_module = compare_module
        self.init_run()
        pass

    def init_run(self):
        """
        根据当前对象,以及选中的模式,初始化要调用的模块和方法
        :return: 实际调用的方法并加入到队列中
        """
        temp=filter(lambda item:item['identify']==self.compare_module,COMPARE_MODULE)
        self.compare_module=list(temp)[0]
        module=self.compare_module['identify']

        if module=='pay_paysys':   # 支付 --> 线上和第三方系统
            self.module_func=run_pay_paysys.runCompare
            pass
        elif module=='pay_offsys':  # 支付--> 线上和线下
            self.module_func = run_pay_offline.runCompare
            pass
        elif module=='mini_wechate':  # 小程序--> 线上和微信
            self.module_func = run_pay_paysys.runCompare
            pass
        elif module=='mini_offsys':  # 小程序--> 线上和线下系统
            self.module_func = run_mapp_offline.run_compare
            pass
        elif module=='mini_saas':  # 小程序--> 线上自营和线上订单明细
            self.module_func=func_nopass
            pass
        elif module=='ota_otasys':  # OTA--> 线上和OTA系统
            self.module_func = run_ota_otasys.runCompare
            pass
        else:
            self.module_func = func_nopass
            pass


        pass

    def run(self):
        """
        把对比方法放入队列中的线程执行
        :return: 无返回值
        """
        self.queue.put('选择的模式为：%s' %self.compare_module['show'])
        self.queue.put('对比文件地址：%s' %self.compare_file)
        self.module_func(self.compare_file,self.queue)
        pass


if __name__ == "__main__":
    """
    程序运行的主方法
    """
    app = App()
    app.title("对账工具")        # 声明窗口的标题
    app.iconbitmap('docom.ico')  # 改变窗口图标

    # 获取屏幕尺寸以计算布局参数，使窗口居屏幕中央
    width = 900
    height = 400
    screenwidth = app.winfo_screenwidth()
    screenheight = app.winfo_screenheight()
    alignstr = '%dx%d+%d+%d' % (width, height, (screenwidth - width) / 2, (screenheight - height) / 2)
    app.geometry(alignstr)

    # 设置窗口是否可变长、宽，True：可变，False：不可变
    app.resizable(width=True, height=True)
    app.mainloop()