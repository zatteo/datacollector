from threading import Thread

from botcrypto.helper import *

class Accuracy(Thread):
	"""
	evaluation of each signal base on the signal value and the next market values with a EMA based on the 6 next values.
	A mark beetween -1 & 1 is given ( -- proportional to the correlation beetween the oscillator & the market move)
	An average of each mark give the indicator accuracy
	Evaluation function use (x, y) -> z (continuous on x, y):
		x = close_price(t+1)/close_price(t) [0;+infini[
		y = signal(t) ]-infini;+infini[
		z = {
			x < 1 -> z = -y/x + y
			x > 1 -> z = x*y - y
		} ]-infini;+infini[

	WORK IN PROGRESS

	"""
	
	def measure_signal(self, signal):
		"""
		Compare the signal strengh to the market 
		Signal > 0 -> Growth
		Signal < 0 -> Fall
		"""
		# Get market growth
		ohlcs = self.web_db.sql_fetchall("SELECT * FROM OHLCS WHERE id_pair = %s ANd interval = %s AND date_ohlc >= %s ORDER BY id LIMIT %s", [signal['id_pair'], signal['interval'], signal['date_ohlc'], self.range])
		if len(ohlcs) != self.range:
			log(sys._getframe().f_code.co_name, "not enough OHLC to get a mark -- ohlcs : "+str(len(ohlcs))+" range : "+str(self.range), self.logs, 'w')
			return
		values = []
		y = signal['signal'] # < || > || == 0
		for i in range(len(ohlcs)-1):
			if i == 0:
				ohlc = parse_ohlc(ohlcs[i])
			else:
				ohlc = next_ohlc
			next_ohlc = parse_ohlc(ohlcs[i+1])
			x = next_ohlc['avg']/ohlc['avg'] # < || > || == 1
			z = 0
			if x < 1 :
				z = -y/x + y
			if x > 1 :
				z = x*y - y
			values.append(z*1000)
		# Compute the EMA of signal evaluation
		mark = ema(values, Decimal(0.1))
		# Save the mark
		self.web_db.sql_commit("UPDATE INDICATOR_SIGNAL SET mark = %s WHERE id = %s", [mark, signal['id']])

	def calcul_accuracy(self):
		""" Select plage of signal datas an compute their accuracy
		TODO - Smart data selection on enver indicator, do fetch all db signals
		"""
		signals = self.web_db.sql_fetchall("SELECT * FROM INDICATOR_SIGNAL", [])
		for signal in signals:
			self.measure_signal(parse_indicator_signal(signal))

	def run(self):
		# Notify about the service activity
		service_activity(self.env, self.db, self.web_db, self.service_name, self.servive_timeout)
		self.calcul_accuracy()

	def __init__(self, env, db, web_db):
		""" Set variables required by the class methods """
		Thread.__init__(self)
		self.range = 9 # Number of market value to consider to give a mark
		self.env = env
		self.service_name = "accuracy"
		self.servive_timeout = 60
		self.db = db
		self.web_db = web_db
		self.logs = Logs(env, self.web_db)
		self.logs.set_service_name("accuracy")
		# Get an array of enabled brokers
		self.brokers = get_brokers(env, self.web_db, self.logs)
		