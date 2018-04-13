import datetime

from botcrypto.helper import *

class Ichimoku(object):
	""" Class computing ichimoku & the oscillator associated """

	def get_clouds_signal(self, current_date, interval, clouds):
		""" Analyse clouds tendency
		Are the clouds under, uppon the price?
		Is the cloud going up/down ?
		Return a integer related to this questions

		current_date : integer
		interval : integer
		clouds : array

		res = 0 -> neutral
		res > 0 -> Growth
		res < 0 -> Fall
		"""
		res = 0
		# Fetch the ohlc at the given date
		current_price = self.web_db.sql_fetchone("SELECT close FROM OHLCS WHERE pair_id = %s AND interval = %s AND date_ohlc = %s LIMIT 1", (self.pair['id'], interval, current_date))
		if(current_price == None):
			log(sys._getframe().f_code.co_name, 'no ohlc data at %s' % date_converter.timestamp_to_string(current_date, "%H:%M"), self.logs, 'b')
			return 0
		current_price = current_price[0]
		# Get the current cloud
		current_cloud_find = False
		for i in range(len(clouds)):
			cloud_date, cloud_top, cloud_bot, top_sum, bot_sum, coef_dir_avg = clouds[i]
			if(current_date in cloud_date):
				current_cloud_find = True
				cloud_index = cloud_date.index(current_date)
				# If the current close price value is in the cloud, return 0
				if(current_price > cloud_bot[cloud_index] and current_price < cloud_top[cloud_index]):
					return 0
				break;
		if(not current_cloud_find):
			log(sys._getframe().f_code.co_name, 'Current cloud not find', self.logs, 'f')
			return 0
		# if the current cloud is the last one
		if(i == len(clouds)-1):
			cloud_length = len(cloud_date)
			# Get the price at the begin and the middle at the end of the cloud
			cloud_start = (cloud_top[0]+cloud_bot[0])/2
			cloud_end = (cloud_top[cloud_length-1]+cloud_bot[cloud_length-1])/2
			# Leading coef
			res = (cloud_end - cloud_start)/cloud_length
			# Affinate leading coef with the clouds top & bot leading coef
			res = ((cloud_end - cloud_start)/cloud_length + coef_dir_avg)/2
			# Cloud is grothwing and we are upon
			if(res > 0 and current_price >= cloud_top[cloud_index]):
				return res
			# Cloud is falling and we are under
			elif(res < 0 and current_price <= cloud_bot[cloud_index]):
				return res
			# Cloud is flat
			else:
				return 0
		else:
			# If there is more than 1 twist, return 0
			if(len(clouds)-1 - i > 1):
				return 0
			# If there is 1 twist
			cloud_length = len(cloud_date)
			# Leading coef
			cloud_start = (cloud_top[0]+cloud_bot[0])/2
			cloud_end = (cloud_top[cloud_length-1]+cloud_bot[cloud_length-1])/2
			# Affinate leading coef with the clouds top & bot leading coef
			res = ((cloud_end - cloud_start)/cloud_length + coef_dir_avg)/2
			# Current cloud center of gravity
			cloud_top_avg = top_sum/len(cloud_date)
			cloud_bot_avg = bot_sum/len(cloud_date)
			cloud_center = cloud_top_avg - cloud_bot_avg
			# Cloud is grothwing and we are upon
			if(res > 0 and current_price >= cloud_top[cloud_index]):
				cloud_date, cloud_top, cloud_bot, top_sum, bot_sum, coef_dir_avg = clouds[i+1]
				# Next cloud center of gravity
				next_cloud_top_avg = top_sum/len(cloud_date)
				next_cloud_bot_avg = bot_sum/len(cloud_date)
				next_cloud_center = next_cloud_top_avg - next_cloud_bot_avg
				# If the next cloud is upon the current one - growth
				if(cloud_center < next_cloud_center):
					return res
				else:
					return 0
			# Cloud is falling and we are under
			elif(res < 0 and current_price <= cloud_bot[cloud_index]):
				cloud_date, cloud_top, cloud_bot, top_sum, bot_sum, coef_dir_avg = clouds[i+1]
				# Next cloud center of gravity
				next_cloud_top_avg = top_sum/len(cloud_date)
				next_cloud_bot_avg = bot_sum/len(cloud_date)
				next_cloud_center = next_cloud_top_avg - next_cloud_bot_avg
				# If the next cloud is under the current - fall
				if(next_cloud_center < cloud_center):
					return res
				else:
					return 0
			# Cloud is flat
			else:
				# If the cloud is under
				if(current_price > cloud_top[cloud_index]):
					cloud_date, cloud_top, cloud_bot, top_sum, bot_sum, coef_dir_avg = clouds[i+1]
					next_cloud_top_avg = top_sum/len(cloud_date)
					next_cloud_bot_avg = bot_sum/len(cloud_date)
					next_cloud_center = next_cloud_top_avg - next_cloud_bot_avg
					# if next cloud is upon
					res = next_cloud_center - cloud_center
					if(res > 0):
						return res;
					else:
						return 0
				# If the cloud is uppon
				elif(current_price < cloud_top[cloud_index]):
					cloud_date, cloud_top, cloud_bot, top_sum, bot_sum, coef_dir_avg = clouds[i+1]
					next_cloud_top_avg = top_sum/len(cloud_date)
					next_cloud_bot_avg = bot_sum/len(cloud_date)
					next_cloud_center = next_cloud_top_avg - next_cloud_bot_avg
					# If next cloud is under
					res = next_cloud_center - cloud_center
					if(res < 0):
						return res;
					else:
						return 0
				else:
					return 0
		return res

	def compute_ichimoku_signal(self, current_date, interval=60):
		""" Compute the oscillator according the ichimoku lines
		
		res = 0 -> neutral
		res > 0 -> Growth
		res < 0 -> Fall
		"""
		res = 0
		# Get the ohlc and the ichimoku datas from 26 periodes behind to 26 periodes after
		date_start = current_date - 26*interval
		date_end = current_date + 26*interval
		ichimoku = self.web_db.sql_fetchall("SELECT * FROM ICHIMOKUS WHERE interval = %s AND pair_id = %s AND date_ohlc >= %s AND date_ohlc <= %s ORDER BY date_ohlc", (interval, self.pair['id'], date_start, date_end))
		if(len(ichimoku) == 0):
			log(sys._getframe().f_code.co_name, "ichimokus datas are missing", self.logs)
			return 0
		else:
			# Get the clouds from ssa & ssb lines
			top_sum = 0
			bot_sum = 0
			coef_dir_avg = 0
			# top define the line upon : ssa or ssb. it's ssab we undifined
			top = 'ssab'
			cloud_top = []
			cloud_bot = []
			cloud_date = []
			clouds = []
			nb_twist = 0
			in_process = False
			for i in range(len(ichimoku)):
				ich = parse_ichimoku(ichimoku[i])
				# Get which line is upon, ssa|ssb
				if(i == 0):
					if (ich['ssa'] == ich['ssb']):
						top = 'ssab'
					elif (ich['ssa'] < ich['ssb']):
						top = 'ssb'
					else:
						top = 'ssa'
				if(ich['top'] == 0 or ich['bot'] == 0):
					log(sys._getframe().f_code.co_name, "%s : top or bot is 0, can't compute signal" % get_readable_date(ich['date_ohlc']), self.logs)
					return 0
				# If top value change -> there is a new cloud
				if((top == 'ssab' and ich['ssa'] != ich['ssb']) or (top == 'ssa' and ich['ssa'] < ich['ssb']) or (top == 'ssb' and ich['ssb'] < ich['ssa'])):
					nb_twist += 1
					# Append the previous cloud to the cloud array
					clouds.append([cloud_date, cloud_top, cloud_bot, top_sum, bot_sum, coef_dir_avg/len(cloud_date)])
					# Reset the data for the next cloud
					if (ich['ssa'] == ich['ssb']):
						top = 'ssab'
					elif (ich['ssa'] < ich['ssb']):
						top = 'ssb'
					else:
						top = 'ssa'
					cloud_date = [ich['date_ohlc']]
					cloud_bot = [ich['bot']]
					cloud_top = [ich['top']]
					top_sum = ich['top']
					bot_sum = ich['bot']
					coef_dir_avg = 0
					in_process = False
				# It is still the same cloud
				else:
					cloud_top.append(ich['top'])
					cloud_bot.append(ich['bot'])
					cloud_date.append(ich['date_ohlc'])
					top_sum += ich['top']
					bot_sum += ich['bot']
					# Clould leading factor average
					coef_dir_avg += (ich['top'] - parse_ichimoku(ichimoku[i-1])['top'] + ich['bot'] - parse_ichimoku(ichimoku[i-1])['bot'])/2
					in_process = True
			if(in_process):
				# Append the current cloud to the cloud array
				clouds.append([cloud_date, cloud_top, cloud_bot, top_sum, bot_sum, coef_dir_avg/len(cloud_date)])
			res = self.get_clouds_signal(current_date, interval, clouds)
		return res

	def ichimoku_plage(self, interval, ohlc):
		""" calcul ichimoku lines from the given ohlc datas
		value 0 in a line means no data for calculation
		"""
		# Ichimoku require at least 9 values
		if(len(ohlc) < 9):
			return []
		# Pre-set a the result array of ichimokus values
		to_insert = [[0 for x in range(6)] for x in range(len(ohlc)+26)]
		for i in range(0, len(ohlc)):
			cur_ohlc = parse_ohlc(ohlc[i])
			to_insert[i][0] = cur_ohlc['date_ohlc']# date_ohlc
			to_insert[i][1] = 0# tenkan
			to_insert[i][2] = 0# kijoun
			to_insert[i][3] = 0# chikou
			to_insert[i][4] = 0# ssa
			to_insert[i][5] = 0# ssb
			to_insert[i+26][0] = cur_ohlc['date_ohlc']+26*interval
			to_insert[i+26][1] = 0
			to_insert[i+26][2] = 0
			to_insert[i+26][3] = 0
			to_insert[i+26][4] = 0
			to_insert[i+26][5] = 0
		# range ohlc array
		for i in range(0, len(ohlc)):
			cur_ohlc = parse_ohlc(ohlc[i])
			#tenkan
			h_9 = cur_ohlc['high']
			b_9 = cur_ohlc['low']
			#kijoun
			h_26 =  cur_ohlc['high']
			b_26 = cur_ohlc['low']
			#chikou
			chikou = cur_ohlc['close']
			#ssa
			ssa = (h_9+b_9+h_26+b_26)/4
			#ssb
			h_52 = cur_ohlc['high']
			b_52 = cur_ohlc['low']
			for l in range(0, 52):
				past = i-l
				if(past < 0):
					break
				past_ohlc = parse_ohlc(ohlc[past])
				#tenkan
				if(past_ohlc['high'] > h_9 and l < 9):
					h_9 = past_ohlc['high']
				if(past_ohlc['low'] < b_9 and l < 9):
					b_9 = past_ohlc['low']
				#kijoun
				if(past_ohlc['high'] > h_26 and l < 26):
					h_26 = past_ohlc['high']
				if(past_ohlc['low'] < b_26 and l < 26):
					b_26 = past_ohlc['low']
				#ssb
				if(past_ohlc['high'] > h_52):
					h_52 = past_ohlc['high']
				if(past_ohlc['low'] < b_52):
					b_52 = past_ohlc['low']
			tenkan = 0
			kijoun = 0
			ssa = 0
			ssb = 0
			if(l >= 8):
				tenkan = (h_9+b_9)/2
			if(l >= 25):
				kijoun = (h_26+b_26)/2
			if(l >= 51):
				ssa = (tenkan+kijoun)/2
				ssb = (h_52+b_52)/2
			to_insert[i][1] = tenkan
			to_insert[i][2] = kijoun
			to_insert[i+26][4] = ssa
			to_insert[i+26][5] = ssb
			if(i >= 26):
				to_insert[i-26][3] = chikou
		return to_insert
		
	def update_or_insert_ichimoku(self, interval, ohlc):
		""" Store ichimoku lines after computing 
		interval : integer
		ohlc : array
		"""
		# Init the array wich containt datas to insert in the db
		to_insert = self.ichimoku_plage(interval, ohlc)
		for i in range(0, len(to_insert)):
			# Store ichimoku lines		
			if not self.web_db.sql_commit("SELECT update_or_insert_ichimoku(%s, %s, %s, %s, %s, %s, %s, %s);", [self.pair['id'], interval, to_insert[i][0], to_insert[i][1], to_insert[i][2], to_insert[i][3], to_insert[i][4], to_insert[i][5]]):
				log(sys._getframe().f_code.co_name, "update_or_insert_ichimoku failed", self.logs, 'b')
				return
		self.web_db.conn().commit();
		log(sys._getframe().f_code.co_name, "%s ichimoku" % len(to_insert), self.logs)

	def update_or_insert_ichimoku_signal(self, interval, ohlc):
		""" Compute ichimoku signal (oscillator) and store it in the db
		interval : interger
		ohlc : array of ohlc
		"""
		for i in range(0, len(ohlc)):
			cur_ohlc = parse_ohlc(ohlc[i])
			# compute the ichimoku signal
			signal = self.compute_ichimoku_signal(cur_ohlc['date_ohlc'], cur_ohlc['interval'])
			# store ichimoku signal
			if not self.web_db.sql_commit("SELECT insert_indicator_signal(%s, %s, text %s, %s, %s);", [self.pair['id'], cur_ohlc['interval'], "ichimoku", cur_ohlc['date_ohlc'], signal]):
				log(sys._getframe().f_code.co_name, "insert_indicator_signal failed", self.logs, 'b')
				return
		self.web_db.conn().commit();
		log(sys._getframe().f_code.co_name, "%s ichimoku signals - last : %s" % (len(ohlc), get_readable_date(cur_ohlc['date_ohlc'])), self.logs)		

	def ichimoku_from_ohlc(self, ohlc = []):
		""" Call function to compute ichimoku related datas.
		It's possible to check & split the given ohlc array in subarray of continus ohlc (date_ohlc). We don't because ohlc appear to be continus.
		"""
		if len(ohlc) == 0:
			log(sys._getframe().f_code.co_name, "ohlc datas are missing", self.logs)
		else:
			interval = parse_ohlc(ohlc[0])['interval']
			# Compute the Ichimoku datas and store in the db
			self.update_or_insert_ichimoku(interval, ohlc)
			# Compute and save the Ichimoku signal
			self.update_or_insert_ichimoku_signal(interval, ohlc)
			log(sys._getframe().f_code.co_name, "Last ohlc "+str(ohlc[-1]), self.logs)
		

	def calcul_ichimoku(self, all_ichimoku = False):
		""" Compute ichimoku datas according the data in the db
		Compare last ichimoku data with ohlc datas for the pair and intervals given
		"""
		for interval in self.intervals:
			# Check the last ichimoku data completed for this interval
			ichimoku = self.web_db.sql_fetchone("SELECT * FROM ICHIMOKUS WHERE tenkan <> 0 AND kijun <> 0 AND chikou <> 0 AND ssa <> 0 AND ssb <> 0  AND pair_id = %s AND interval = %s ORDER BY date_ohlc DESC LIMIT 1", [self.pair['id'], interval])
			# If we don't have ichimoku datas or we want to compute all ichimoku datas
			if ichimoku == None or all_ichimoku:
				# Get all the ohlc values for this interval
				ohlc = self.web_db.sql_fetchall("SELECT * FROM OHLCS WHERE pair_id = %s AND interval=%s ORDER BY date_ohlc", [self.pair['id'], interval])
			else :
				ichimoku = parse_ichimoku(ichimoku)
				# Get ohlc datas to compute new ichimoku datas since the last ichimoku date
				ohlc = self.web_db.sql_fetchall("SELECT * FROM OHLCS WHERE pair_id=%s AND date_ohlc >= %s AND interval = %s ORDER BY date_ohlc", [self.pair['id'], ichimoku['date_ohlc']-53*interval, interval])
			# Compute ichimoku datas from the fecthed ohlc
			self.ichimoku_from_ohlc(ohlc)

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

	def __init__(self, web_db, logs, intervals = [1*60]):
		""" Set variables required by the class methods """
		self.web_db = web_db
		self.logs = logs
		self.intervals = intervals
		self.pair = None