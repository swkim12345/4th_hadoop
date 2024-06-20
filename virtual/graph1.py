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
df['거래량_표준편차1']=df['거래량_표준편차1'].map(lambda x: double(x))
df['거래량_표준편차2']=df['거래량_표준편차2'].map(lambda x: double(x))
df["증감"]=df['거래량_표준편차2']-df['거래량_표준편차1']
print(len(df['거래량_표준편차1']))
# for i in range(len(df)):
#     if(df["증감"][i]<0):
#         print(df["증감"][i])
# 실제 값 (가격 증감)
#df = pd.DataFrame(data, columns=['기업코드', '거래량_표준편차1', '거래량_표준편차2', '가격_표준편차1', '가격_표준편차2', '증감'])



# 막대 그래프 그리기
#plt.figure(figsize=(10, 6))

# 증감값에 따라 막대 그래프 그리기
x = np.arange(len(df['기업코드']))
plt.bar(x, df['증감'], color='green', label='증감',width=8)

# x 축 레이블, 제목 설정
plt.xlabel('company')
plt.ylabel('differ')
plt.title('stock price vs 30 stock price')
plt.ylim([-10000, 10000])
# x 축 눈금 설정
plt.xticks(x, df['증감'])

# 수평선 추가 (증감이 0인 지점)
plt.axhline(y=0, color='black', linewidth=1)

# 그리드 추가

plt.show()
# # 기업코드를 인덱스로 설정
# df.set_index('기업코드', inplace=True)
#
# # 막대 그래프 그리기
# plt.figure(figsize=(10, 6))
#
# # 증감값에 따라 막대 그래프 그리기
# plt.bar(df.index, df['증감'], color='blue', label='증감')
#
# # x 축 레이블, 제목 설정
# plt.xlabel('기업코드')
# plt.ylabel('거래량 증감')
# plt.title('기업별 거래량 증감')
#
# # x 축 눈금 설정
# plt.xticks(df.index)
#
# # 수평선 추가 (증감이 0인 지점)
# plt.axhline(y=0, color='r', linewidth=1)
#
# # 그리드 추가
# plt.grid(True)
#
# # 범례 추가
# plt.legend()
#
# # 그래프 보여주기
# plt.tight_layout()
# plt.show()