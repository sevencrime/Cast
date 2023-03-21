import pandas as pd

# 创建一个示例 DataFrame
df = pd.DataFrame({
    'A': [1, 2, 3, 4],
    'B': [1, 2, 4, 4],
    'C': [3, 4, 5, 6],
    'D': [4, 5, 6, 7],
    'E': [1, 2, 3, 4]
})

# 定义一个函数，用于将 A 和 B 列的不同值设置为红色
def highlight_diff(x):
    # 如果 A 和 B 不相等，则返回一组样式
    if x['A'] != x['B']:
        return ['color: red', 'color: red']
    # 如果 A 和 B 相等，则返回空样式
    return ['', '']

# 使用 apply 方法将样式应用到 DataFrame 中的每个单元格
styled_df = df.style.apply(highlight_diff, subset=['A','B'], axis=1)

# 将带有样式的 DataFrame 导出为 CSV 文件
styled_df.to_excel('output.xlsx', index=False)
