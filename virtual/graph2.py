import numpy as np
import pandas as pd
from numpy import double
from sklearn.metrics import precision_score, recall_score, f1_score
import matplotlib.pyplot as plt
# 텍스트 파일 경로
file_path = 'volume_stock'


# 텍스트 파일 읽기
data = []
with open(file_path, 'r') as file:
    for idx, line in enumerate(file):
        parts = line.strip().split()
        기업코드 = idx
        거래량_표준편차1 = parts[1].split(',')[0]
        거래량_표준편차2 = parts[1].split(',')[1]
        가격_표준편차1 = parts[1].split(',')[2]
        가격_표준편차2 = parts[1].split(',')[3]
        data.append([기업코드, 거래량_표준편차1, 거래량_표준편차2,가격_표준편차1,가격_표준편차2])

df = pd.DataFrame(data, columns=['기업코드', '거래량_표준편차1', '거래량_표준편차2','가격_표준편차1','가격_표준편차2'])

# 가격증감과 호재성을 boolean 값으로 변환
df['기업코드']=df['기업코드'].map(lambda x: int(x))
df['가격_표준편차1']=df['가격_표준편차1'].map(lambda x: double(x))
df['가격_표준편차2']=df['가격_표준편차2'].map(lambda x: double(x))
df["증감"]=df['가격_표준편차1']-df['가격_표준편차2']
print(len(df['증감']))
for i in range(len(df)):
    print(df['증감'][i])
x = np.arange(len(df['기업코드']))
plt.bar(x, df['증감'], color='green', label='증감',width=8)

# x 축 레이블, 제목 설정
plt.xlabel('company')
plt.ylabel('differ')
plt.title('stock price vs 30 stock price')
plt.ylim([-1000, 1000])
# x 축 눈금 설정
plt.xticks(x, df['증감'])

# 수평선 추가 (증감이 0인 지점)
plt.axhline(y=0, color='black', linewidth=1)
plt.show()
# 실제 값 (가격 증감)
# y_true = df['가격_표준편차1']
#
# # 예측 값 (호재성)
# y_pred = df['가격_표준편차2']
#
# plt.figure(figsize=(8, 6))
# plt.scatter(y_true, y_pred, color='blue', label='Data Points')
# plt.xlim([0, 100])      # X축의 범위: [xmin, xmax]
# plt.ylim([0, 100])
# # x, y 축 레이블과 제목 추가
# plt.xlabel('30 stock price')
# plt.ylabel('15 stock price')
# plt.title('stock price vs 30 stock price')
# plt.plot([0, 1000], [0, 1000], color='red', linestyle='--', label='y=x Line')
# # 그리드 추가
# plt.grid(True)
#
# # 범례 추가
# plt.legend()
#
# # 그래프 보여주기
# plt.show()