import pandas as pd
from pyecharts.charts import Bar
from pyecharts import options as opts
from pyecharts.render import make_snapshot
# from snapshot_selenium import snapshot
from jinja2 import Template


# 创建数据表格
def create_table():
    data = {
        'Name': ['Alice', 'Bob', 'Charlie', 'David'],
        'Age': [25, 30, 35, 40],
        'City': ['New York', 'London', 'Paris', 'Tokyo']
    }

    df = pd.DataFrame(data)
    return df


# 创建柱状图
def create_bar_chart():
    x_data = ["A", "B", "C", "D", "E"]
    y_data = [10, 20, 30, 40, 50]

    bar = (
        Bar()
            .add_xaxis(x_data)
            .add_yaxis("柱状图", y_data)
            .set_global_opts(title_opts=opts.TitleOpts(title="柱状图示例"))
    )

    return bar


# 生成网页
def generate_html():
    df = create_table()
    bar = create_bar_chart()

    # 使用Tabulator生成表格的HTML代码
    table_html = df.to_html(classes='table', index=False)

    # 使用pyecharts生成柱状图的HTML代码
    bar_html = bar.render_embed()

    # 读取HTML模板文件
    with open('template.html', 'r') as file:
        template_content = file.read()

    # 使用Jinja2模板引擎渲染HTML模板
    template = Template(template_content)
    rendered_html = template.render(table_html=table_html, bar_html=bar_html)

    # 将渲染后的HTML保存到文件
    with open('output.html', 'w') as file:
        file.write(rendered_html)

    print("网页生成成功！")


# 运行生成网页函数
generate_html()
