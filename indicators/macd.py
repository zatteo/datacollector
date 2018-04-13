import datetime

from botcrypto.helper import *

class Macd(object):
	""" Class computing macd & the oscillator associated """

	def compute_macd_signal(self, current_date, interval=60):
		""" Compute the macd oscillator
		The last 9 macd values are considered and compare to get a ema of leading factor on macd histograms ~ res
		res = 0 -> neutral
		res > 0 -> Growth
		res < 0 -> Fall
		"""
		res = 0
		date_start = current_date - 9*interval
		date_end = current_date
		# Get the last MACD values
		macds = self.web_db.sql_fetchall("SELECT date_ohlc, macd, signal FROM MACDS WHERE pair_id = %s AND interval = %s AND date_ohlc >= %s AND date_ohlc <= %s AND macd IS NOT NULL AND signal IS NOT NULL ORDER BY date_ohlc DESC", [self.pair['id'], interval, date_start, date_end])
		macds = macds[::-1]
		if(len(macds) == 0):
			log(sys._getframe().f_code.co_name, "macd datas are missing", self.logs)
			return res
		# Get the very last histogramme tendance (from the last 0)
		macd = 0
		signal = 0
		macd_hist_coef_dir = []
		for i in range(1, len(macds)):
			macd = macds[i][1]
			signal = macds[i][2]
			macd_hist_coef_dir.append((macd - signal) - (macds[i-1][1] - macds[i-1][2]))
		# Exponential mobile average on the macd_hist_coef_dir
		coef_dir = ema(macd_hist_coef_dir)
		# If the last macd histo is positive and histograms and growing, signal is positive
		if(macd - signal > 0 and coef_dir > 0):
			res = coef_dir
		# If the last macd histo is negative and histograms and falling, signal is negative
		elif(macd - signal < 0 and coef_dir < 0):
			res = coef_dir
		return res

	def update_or_insert_macd_signal(self, interval, ohlc):
		""" Compute and store the macd signals for the given ohlc array """
		signal_compute = 0
		# For every ohlc data, compute and store the macd sell/buy signal
		for i in range(0, len(ohlc)):
			cur_ohlc = parse_ohlc(ohlc[i])
			# compute the macd signal
			signal = self.compute_macd_signal(cur_ohlc['date_ohlc'], cur_ohlc['interval'])
			# save the macd signal
			if not self.web_db.sql_commit("SELECT insert_indicator_signal(%s, %s, text %s, %s, %s);", [self.pair['id'], cur_ohlc['interval'], "macd", cur_ohlc['date_ohlc'], signal]):
				log(sys._getframe().f_code.co_name, "insert_indicator_signal failed", self.logs, 'b')
				return signal_compute
			signal_compute += 1
		return signal_compute
		

	def macd_from_ohlc(self, interval, ohlcs):
		""" Compute macd values from the given ohlc array """
		macd_compute = 0
		for n in range(0, len(ohlcs)):
			ohlc = parse_ohlc(ohlcs[n])
			new_macd_date = ohlc['date_ohlc']
			# Calcul exponential mobile average for each periodes
			periodes = [12, 26]
			ema = [0, 0]
			for p in range(0, len(periodes)):
				periode = periodes[p]
				# Get the previous ohlc datas require to compute macd values for this date
				ohlcs_ema = self.web_db.sql_fetchall("SELECT * FROM OHLCS WHERE pair_id = %s AND interval = %s AND date_ohlc <= %s ORDER BY date_ohlc DESC LIMIT %s", [self.pair['id'], interval, new_macd_date, periode])
				if len(ohlcs_ema) < periode:
					log(sys._getframe().f_code.co_name, "ohlc datas are missing at %s" % get_readable_date(new_macd_date), self.logs)
					break
				ohlcs_ema = ohlcs_ema[::-1]
				cte_gliss = Decimal(2./(periode+1.))
				# Compute the ema accoring to fecthed ohlcs
				ohlc_ema = parse_ohlc(ohlcs_ema[0])
				ema[p] = ohlc_ema['close']
				for i in range(1, periode):
					ohlc_ema = parse_ohlc(ohlcs_ema[i]);
					# Hunter
					# print 'i : '+str(i)+', ema : '+str(ema[p])+', cte_gliss : '+str(cte_gliss)+', close : '+strohlc_ema['close'])
					ema[p] = ema[p]+cte_gliss*(ohlc_ema['close']-ema[p])
					# print "n = "+str(n)+" EMA FOR PERIODE "+str(periode)+" EQUAL "+str(ema[p])
			if ema[0] != 0 and ema[1] != 0:
				# Save the macd values for the new date
				# MACD = EMA 12 - EMA 26
				macd = ema[0]-ema[1]
				# Compute the signal line : EMA of the MACD for 9 perdiodes
				macd_ema = None
				# Get the 8 previous macd to get 9 macd values
				macd_val = self.web_db.sql_fetchall("SELECT macd FROM MACDS WHERE pair_id = %s AND interval = %s AND date_ohlc < %s ORDER BY date_ohlc DESC LIMIT 8", [self.pair['id'], interval, new_macd_date])
				if(len(macd_val) == 8):
					cte_gliss = Decimal(2./(10.+1.))
					#init
					macd_ema = macd_val[len(macd_val)-1][0]
					# print "n:"+str(n)+", macd:"+str(macd)+", cte_glisse:"+str(cte_gliss)
					for m in range(len(macd_val)-2, 0, -1):
						# print "macd_ema : "+str(macd_ema)+", macd_val : "+str(macd_val[m][0])
						macd_ema = macd_ema + cte_gliss*(macd_val[m][0] - macd_ema)
					macd_ema = macd_ema + cte_gliss*(macd - macd_ema)
					# print "macd_ema : "+str(macd_ema)
					if not self.web_db.sql_commit("SELECT insert_macd(%s, %s, %s, %s, %s)", [self.pair['id'], interval, new_macd_date, macd, macd_ema]):
						log(sys._getframe().f_code.co_name, "insert_macd failed", self.logs, 'b')
						return macd_compute
					macd_compute += 1
		return macd_compute

	def calcul_macd(self, all_data=False):
		""" Compute macd related datas according the current ohlc datas in the db """
		for interval in self.intervals:
			# Check the last macd value for these interval
			last_macd = self.web_db.sql_fetchone("SELECT * FROM MACDS WHERE pair_id = %s AND interval = %s ORDER BY date_ohlc DESC LIMIT 1", [self.pair['id'], interval]);
			if(last_macd == None):
				all_data = True
			if(all_data):
				# Fetch all ohlc data for this pair & interval
				ohlcs = self.web_db.sql_fetchall("SELECT * FROM OHLCS WHERE pair_id = %s AND interval = %s ORDER BY date_ohlc", [self.pair['id'], interval])
				log(sys._getframe().f_code.co_name, "Compute all the macd data.. (%s)" % len(ohlcs), self.logs, 'e')
			else:
				# Fetch ohlc data require to compute new macd datas
				last_macd = parse_macd(last_macd)
				from_date = last_macd['date_ohlc'];
				ohlcs = self.web_db.sql_fetchall("SELECT * FROM OHLCS WHERE pair_id = %s AND interval = %s AND date_ohlc > %s ORDER BY date_ohlc", [self.pair['id'], interval, from_date])
			# Compute and store the macd datas according to the ohlc fetched
			macd_compute = self.macd_from_ohlc(interval, ohlcs)
			log(sys._getframe().f_code.co_name, "%s macd" % macd_compute, self.logs)
			# Compute and save the macd signal
			macd_signal_compute = self.update_or_insert_macd_signal(interval, ohlcs)
			log(sys._getframe().f_code.co_name, "%s macd signal" % macd_signal_compute, self.logs)

	def set_pair(self, pair):
		""" Set pair to consider
		pair : parsed
		"""
		self.pair = pair

	def set_intervals(self, intervals):
		""" Set intervals to consider
		interval : integer array
		"""
		self.intervals = intervals

	def __init__(self, web_db, logs, intervals = [60]):
		""" Set variables required by the class methods """
		self.web_db = web_db
		self.logs = logs
		self.pair = None
		self.intervals = intervals