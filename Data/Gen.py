# Number of iterations: 20
# Small dataset: 20*10 = 200 rows
# Medium dataset: 20*100 = 2000 rows
# Large dataset: 20*1000 = 20000 rows


import random

def writer(fname):
	f = open(fname, 'w')
	for i in range(1, 20000):
		col1 = random.randint(40, 70)
		col2 = random.randint(0, 50)
		f.write(str(i)+' '+str(col1)+' '+str(col2)+'\n')
	f.close()

def main():
	writer('large.txt')

if __name__ == '__main__':
	main()
