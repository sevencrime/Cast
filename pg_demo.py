import pandas as pd

# data = [['a','1'],
#         ['a','2'],
#         ['a','3'],
#         ['b','4'],
#         ['b','5'],
#         ['a','6'],
#         ['a','7'],
#         ['c','8'],
#         ['c','9'],
#         ['b','10'],
#         ['b','11']
#         ]
#
data = [['促销话术','1', '第一']
        ['促销话术','2', '第二'],
        ['促销话术','3', '第三'],
        ['吸粉话术','4', '第四'],
        ['吸粉话术','5', '第四'],
        ['促销话术','6', '第六'],
        ['促销话术','7', '第七'],
        ['卖点介绍','8', '第八'],
        ['卖点介绍','9', '第九'],
        ['卖点介绍','10', '你好'],
        ['促销话术','11', '不好']
        ]

df = pd.DataFrame(data,columns=['key','value', 'name'])
col = df['key']
df['token'] = (col != col.shift()).cumsum()
data = df.groupby(['token']).aggregate(lambda x: list(x))
print(data)
data['key'] = data['key'].apply(lambda set:set.pop())
data['value'] = data['value'].apply(lambda set:','.join(set))
data['name'] = data['name'].apply(lambda set:','.join(set))
print(data)
