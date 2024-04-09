from django.db import models

# Create your models here.
class viewed_sp(models.Model): #创建用户信息表
    eid=models.PositiveIntegerField(verbose_name='eid',default=0)
    name =models.CharField(max_length=100,verbose_name='name')
    choices0 = (
        (1, '普通属性'),
        (2, '度量属性(聚合值)'),
    )
    type = models.PositiveIntegerField(choices=choices0, default=1,verbose_name='type')
    rank = models.PositiveIntegerField(verbose_name='rank',default=0)
    choices1 = (
        (0, '不显示'),
        (1, '显示'),
    )
    state = models.PositiveIntegerField(choices=choices1, default=0,verbose_name='state')
    create_date = models.DateField(auto_now_add=True,verbose_name='created')

class report_task(models.Model): #创建报告任务表
    BatchId = models.CharField(max_length=50)
    UseModel = models.CharField(max_length=255)
    ReportName = models.CharField(max_length=255)
    DateRange = models.CharField(max_length=50)
    Status = models.IntegerField()
    UpdateTime = models.DateTimeField()
    PersonInCharge = models.CharField(max_length=100, verbose_name="当前负责人")
    fileUrl = models.CharField(max_length=255)

    def __str__(self):
        return self.ReportName
