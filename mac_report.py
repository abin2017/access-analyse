import pandas as pd
import matplotlib.pyplot as plt


# 创建示例数据
categories = ['A', 'B', 'C', 'D']
values = [20, 35, 30, 15]

# 生成柱状图
plt.bar(categories, values)

# 添加标题和标签
plt.title('柱状图示例')
plt.xlabel('类别')
plt.ylabel('值')

# 显示图表
plt.show()

# 创建示例数据
data = {
    '姓名': ['Alice', 'Bob', 'Charlie', 'David'],
    '年龄': [25, 30, 35, 40],
    '工资': [5000, 6000, 7000, 8000]
}
df = pd.DataFrame(data)

# 生成统计报表
report = df.describe()

# 绘制柱状图
df.plot(x='姓名', y='工资', kind='bar')

# 保存为PDF文件
plt.savefig('report.pdf')

# 保存为HTML文件
plt.savefig('report.html')

