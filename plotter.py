import matplotlib.pyplot as plt
import json

articles = ["wordsCount.json"]

def plotFile(name):
	content = open(name, 'r').read()
	wordsCount = json.loads(content)
	plt.plot(sorted(wordsCount.values()))
	plt.show()

def main():
	
	plotFile(articles[0])


if __name__ == '__main__':
	main()