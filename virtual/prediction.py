import pandas as pd
from sklearn.metrics import precision_score, recall_score, f1_score

# 텍스트 파일 경로
file_path = 'price_stock_hoze'

# 텍스트 파일 읽기
data = []
with open(file_path, 'r') as file:
    for line in file:
        parts = line.strip().split()
        기업코드 = parts[0]
        가격증감 = parts[1].split(',')[0]
        호재성 = parts[1].split(',')[1]
        data.append([기업코드, 가격증감, 호재성])

df = pd.DataFrame(data, columns=['기업코드', '가격증감', '호재성'])

# 가격증감과 호재성을 boolean 값으로 변환
df['가격증감'] = df['가격증감'].map({'TRUE': True, 'FALSE': False})
df['호재성'] = df['호재성'].map({'TRUE': True, 'FALSE': False})

# 실제 값 (가격 증감)
y_true = df['가격증감']

# 예측 값 (호재성)
y_pred = df['호재성']

temp1=0
temp2=0
temp3=0
temp4=0
# 정밀도, 재현율, F1 스코어 계산
for i in range(len(y_true)):
    print(",",y_true[i], ",",y_pred[i],",")
    if y_true[i] == True and y_pred[i]==True:
        temp1+=1
    elif y_true[i] == False and y_pred[i]==True:
        temp2+=1
    elif y_true[i] == True and y_pred[i]==False:
        temp3+=1
    else:
        temp4+=1
print("temp1 = ",temp1)
print("temp2 = ",temp2)
print("temp3 = ",temp3)
print("temp4 = ",temp4)
print(temp1+temp2+temp3+temp4)
# precision = precision_scor(y_true, y_pred)e(y_true, y_pred)
# recall = recall_score
# f1 =
# f1_score(y_true, y_pred)
#
# print("Precision:", precision)
# print("Recall:", recall)
# print("F1 Score:", f1)