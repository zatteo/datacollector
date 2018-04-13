from botcrypto.extern_api import *
from indicators.ichimoku import *
from indicators.macd import *

from threading import Thread

class DataCollector(Thread):
	""" Collect market datas from broker and computes indicators """

	def save_ohlc(self, pair, interval, ohlcs):
		""" Save the ohlc data in the database
			pair: parsed
			interval: integer
			ohlcs: array parsed
		"""
		# We modify the first element with the correct volume
		self.web_db.sql_commit("UPDATE ohlcs SET volume = %s WHERE pair_id = %s AND interval = %s AND date_ohlc = %s", [Decimal(ohlcs[0][5]), pair['id'], interval, int(ohlcs[0][0])]);
		# We skip the first element to add only new elements
		for ohlc in ohlcs[1:]:
			# olhc : [ CloseTime, OpenPrice, HighPrice, LowPrice, ClosePrice, Volume ]
			self.web_db.sql_commit("SELECT insert_ohlc(%s, %s, %s, %s, %s, %s, %s, %s);", [pair['id'], interval, int(ohlc[0]), Decimal(ohlc[1]), Decimal(ohlc[2]), Decimal(ohlc[3]), Decimal(ohlc[4]), Decimal(ohlc[5])]);

	def get_ohlc(self, api, pair, interval, broker):
		""" Fetch the last ohlc in the db and call API to complete with the most recent datas
			api: Instance of the broker's api
			pair: parsed
			interval: integer
			broker: parsed
		"""
		# Get the last ohlc date we have for the concerned broker, pair & inteval
		success = False
		last_ohlc = self.web_db.sql_fetchall("SELECT date_ohlc FROM OHLCS WHERE pair_id = %s AND interval = %s ORDER BY date_ohlc DESC LIMIT 2", [pair['id'], interval])
		# We do not have ohlc -> fecth a maximum of datas
		if len(last_ohlc) == 0:
			last_date = 0
			params = {'broker': broker, 'pair':pair, 'interval':interval}
		# We have 1 ohlc -> fetch the ohlc from this date
		elif len(last_ohlc) == 1:
			last_date = last_ohlc[0][0]
			params = {'broker': broker, 'pair':pair, 'interval':interval, 'since':last_date}
		# We have ohlcs -> fetch the ohlc from the oldest-1 to get volume
		else:
			last_date = last_ohlc[1][0]
			params = {'broker': broker, 'pair':pair, 'interval':interval, 'since':last_date}
		data_age = get_current_date_unix() - last_date
		# Check if the last data ohlc we have is less than 1 minutes ago and update if more
		while data_age > interval and not success:
			# Call the api to get the ohlcs
			ohlcs = []
			while ohlcs == []:
				try:
					api_rep = api.get_ohlc(params)
					while(api_rep['result'] == []):
						if api_rep['error'] != []:
							raise ValueError(str(api_rep['error']))
						api_rep = api.get_ohlc(params)
					ohlcs = api_rep['result']
				except Exception as e:
					log(sys._getframe().f_code.co_name, "Call api error : %s - retry" % e, self.logs, 'w')
				time.sleep(3)
			# Check api reply validity - prevent broker response error on date
			for i in range(len(ohlcs)-1, 0, -1):
				ohlc = ohlcs[i]
				last_date = ohlc[0]
				# Filter date in the futur
				if(last_date > get_current_date_unix()+60):
					log(sys._getframe().f_code.co_name, "OHLC %s [%s] on %s, data : %s -- date ohlc : %s" % (pair['name'], interval, broker['name'], i, get_readable_date(last_date)), self.logs)
					if len(ohlcs) > 0:
						ohlcs = ohlcs[:-1]
				else:
					break;
			if len(ohlcs) > 0:
				log(sys._getframe().f_code.co_name, "+ %s ohlc" % len(ohlcs), self.logs)
				# Store ohlcs we got
				self.save_ohlc(pair, interval, ohlcs)
				success = True
		return success

	def run(self):
		"""  Thread service execution """
		# Notify about the service execution
		new_service(self.env, self.core_db, self.web_db, self.service_name, self.servive_timeout)
		# Check if the service is to stop
		to_stop = service_to_stop(self.env, self.core_db)
		while not to_stop:
			# Get the current date to measure excution time
			current_date = time.time()
			# Fetch available brokers with their pairs from the db
			self.brokers = get_brokers(self.env, self.web_db, self.logs)
			# Fetch available interval we are working with
			self.intervals = get_intervals(self.env, self.web_db, self.logs)
			# Setup indicators parameters
			self.ichimoku.set_intervals(self.intervals)
			self.macd.set_intervals(self.intervals)
			for broker in self.brokers:
				# Select the api to call
				if broker['name'] == "kraken":
					api = self.kraken_api
				elif broker['name'] == "binance":
					api = self.binance_api
				else:
					api = self.cryptowatch_api
				for pair in broker['pair']:
					# Setup indicators parameters
					self.ichimoku.set_pair(pair)
					self.macd.set_pair(pair)
					for interval in self.intervals:
						# Check the oldest data in the db and call the broker API
						self.get_ohlc(api, pair, interval, broker)
					# Compute indicators and signals
					self.ichimoku.calcul_ichimoku()
					self.macd.calcul_macd()
			# Notify about the service execution
			duree = time.time() - current_date
			log(sys._getframe().f_code.co_name, "DATA COMPUTE IN "+str(duree), self.logs)
			service_activity(self.env, self.core_db, self.web_db, duree)
			if duree < self.servive_timeout:
				time.sleep(self.servive_timeout-duree)
			to_stop = service_to_stop(self.env, self.core_db)
		# Close databases connections
		self.core_db.close()
		self.web_db.close()

	def __init__(self, env, core_db, web_db):
		""" Setup variables required by the service """
		Thread.__init__(self)
		self.env = env
		self.service_name = "datacollector"
		self.servive_timeout = 60
		# Database accessors
		self.core_db = core_db
		self.web_db = web_db
		# Log system
		self.logs = Logs(env, self.web_db)
		self.logs.set_service_name("datacollector")
		# Intervals and brokers we works on - completed later
		self.intervals = []
		self.brokers = []
		# Setup the supported API to collect data from
		self.cryptowatch_api = ExternApi(self.logs, "cryptowatch")
		self.kraken_api = ExternApi(self.logs, "kraken", env.kraken_market_public, env.kraken_market_private)
		self.binance_api = ExternApi(self.logs, "binance", env.binance_market_public, env.binance_market_private)
		# Instance class for indicators computation
		self.ichimoku = Ichimoku(self.web_db, self.logs, self.intervals)
		self.macd = Macd(self.web_db, self.logs, self.intervals)