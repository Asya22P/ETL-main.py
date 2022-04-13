# ETL-main.py
import pandas as pd
from pandas import ExcelWriter
from pandas import ExcelFile

import jaydebeapi
from datetime import datetime
import os
import app




cursor = connect.cursor()


def load_stg(path_of_directory):
	terminal = []
	passport = []
	transactions = []

	for i in os.listdir(path_of_directory):
		if i == 'ojdbc7.jar':
			continue

		if i.find(".txt"):
			transactions = pd.read_csv(path_of_directory + i, sep=';', encoding='ANSI', on_bad_lines='skip')

		if i.find("passport") != -1:
			list_file = [i, i.split('_')[1], i.split('_')[-1].split('.')[0], i.split('_')[-1].split('.')[1]]
			passport = pd.read_excel(path_of_directory + i, sheet_name=list_file[1])

		if i.find("terminal") != -1:
			list_file = [i, i.split('_')[0], i.split('_')[-1].split('.')[0], i.split('_')[-1].split('.')[1]]
			terminal = pd.read_excel(path_of_directory + i, sheet_name=list_file[1])

	# print(passport)
	# print(terminal)
	# print(transactions)
	# exit(1)


	files = df_files.loc[(df_files['type'] == 'transactions') & (df_files['extension'] == 'txt'), ['file','date']] 
	global file_name_transactions
	file_name_transactions = files.iat[0, 0]
	date = files.iat[0, 1]



	cursor.execute('alter session set nls_date_format = "DD.MM.RR"')
	cursor.execute('alter session set nls_timestamp_format = "DD.MM.RR HH24:MI:SSXFF"')

	df = pd.read_csv(path_of_directory + file_name_transactions, delimiter=';', header=0, index_col=None)
	df['transaction_date'] = pd.to_datetime(df['transaction_date'], format="%Y-%m-%d %H:%M:%S")
	df['transaction_date'] = df['transaction_date'].dt.strftime("%d-%m-%Y %H:%M:%S")
	
	print(df.info())
	print(df.values.tolist())
	print(df.values.astype('str').tolist())


	cursor.executemany('''
		INSERT INTO de3hd.s_21_trnsctns
		(trans_id, trans_date, amount, card_num, oper_type, oper_result, terminal)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	''', df.values.astype('str').tolist())


	files = df_files.loc[(df_files['type'] == 'terminals') & (df_files['extension'] == 'xlsx'), ['file','date']]
	global file_name_terminals
	file_name_terminals = files.iat[0, 0]
	date = files.iat[0, 1]


	df = pd.read_excel(path_of_directory + file_name_terminals, header=0, index_col=None)
	cursor.executemany('''
		INSERT INTO de3hd.s_21_trmnls(terminal_id, terminal_type, terminal_city, terminal_address)
		VALUES (?, ?, ?, ?)
	''', df.values.tolist())


	files = df_files.loc[(df_files['type'] == 'passport') & (df_files['extension'] == 'xlsx'), ['file','date']]
	global file_name_passport
	file_name_passport = files.iat[0, 0]
	date = files.iat[0, 1]

	df = pd.read_excel(path_of_directory + file_name_passport)
	df['date'] = df['date'].dt.strftime("%d-%m-%Y")
	cursor.executemany('''
		INSERT INTO de3hd.s_21_pssprt_blclst(entry_dt, passport_num)
		VALUES (?, ?)
	''', df.values.astype('str').tolist())



def move_file(path_of_directory, path_to_directory):
	os.replace(path_of_directory + file_name_transactions, path_to_directory + file_name_transactions + '.backup')
	os.replace(path_of_directory + file_name_terminals, path_to_directory + file_name_terminals + '.backup')
	os.replace(path_of_directory + file_name_passport, path_to_directory + file_name_passport + '.backup')


def entry_fact():
	cursor.execute('''
		INSERT INTO de3hd.s_21_real_trnsctns
		SELECT 
		*
		FROM de3hd.s_21_trnsctns
	''')


def entry_hist():
	cursor.execute('''
		CREATE TABLE de3hd.s_21_pssprt_blclst_new as
			SELECT 
				t1.passport_num,
				t1.entry_dt
			FROM de3hd.s_21_pssprt_blclst t1
			LEFT JOIN de3hd.s_21_dwh_dim_pssprt_bl_hist t2
				ON t1.passport_num = t2.passport_num 
				AND t2.deleted_flg = 0
				AND current_timestamp BETWEEN t2.effective_from_dttm AND t2.effective_to_dttm
			WHERE t2.passport_num is null
	''')

	cursor.execute('''
		CREATE TABLE de3hd.s_21_stg_del_tmp_pssprt_bl as
			SELECT 
				t1.passport_num,
				t1.entry_dt
			FROM de3hd.s_21_dwh_dim_pssprt_bl_hist t1
			LEFT JOIN de3hd.s_21_pssprt_blclst t2
				ON t1.passport_num = t2.passport_num 
				AND t1.deleted_flg = 0
				AND current_timestamp BETWEEN t1.effective_from_dttm AND t1.effective_to_dttm
			WHERE t1.passport_num is null
	''')

	cursor.execute('''
		UPDATE de3hd.s_21_dwh_dim_pssprt_bl_hist
		SET effective_to_dttm = TRUNC(SYSDATE - 1)
		WHERE passport_num IN 
			(SELECT passport_num FROM de3hd.s_21_stg_del_tmp_pssprt_bl)
		AND effective_to_dttm = to_date('2999-12-31', 'YYYY.MM.DD') 
	''')


	cursor.execute('''
		INSERT INTO de3hd.s_21_dwh_dim_pssprt_bl_hist (entry_dt, passport_num, effective_from_dttm)
		SELECT 
			entry_dt,
			passport_num,
			entry_dt
		FROM de3hd.s_21_pssprt_blclst_new
	''')

def create_report():
	cursor.execute('''
		INSERT INTO de3hd.s_21_rep_fraund_new(event_dt, passport, fio, phone, event_type)
			SELECT
				t1.trans_date AS event_dt,
				t4.passport_num AS passport,
				(t4.last_name||' '||t4.first_name||' '||t4.patrinymic) AS fio,
				t4.phone,
				'Операция с недействительным пасспортом' AS event_type
			FROM de3hd.s_21_trnsctns t1
			INNER JOIN de3hd.s_21_dwh_cards t2 ON t1.card_num = trim(t2.card_num)
			INNER JOIN de3hd.s_21_dwh_clients t4 ON t3.client = t4.client_id
			INNER JOIN de3hd.s_21_dwh_clients t3 ON t2.account_num = t3.account_num
			WHERE (t4.passport_num IN (
				SELECT 
					passport_num
				FROM de3hd.s_21_pssprt_blcls
				)
			) OR t4.passport_valid_to + INTERVAL '0 23:59:59' DAY TO SECOND < t1.trans_date
			ORDER BY event_dt
	''')
	cursor.execute('''
		INSERT INTO de3hd.s_21_rep_fraund_new(event_dt, passport, fio, phone, event_type)
			SELECT
				t1.trans_date AS event_dt,
				t4.passport_num AS passport,
				(t4.last_name||' '||t4.first_name||' '||t4.patrinymic) AS fio,
				t4.phone,
				'Договор не действителен' AS event_type
			FROM de3hd.s_21_trnsctns t1
			INNER JOIN de3hd.s_21_dwh_cards t2 ON t1.card_num = trim(t2.card_num)
			INNER JOIN de3hd.s_21_dwh_clients t3 ON t2.account_num = t3.account_num
			INNER JOIN de3hd.s_21_dwh_clients t4 ON t3.client = t4.client_id
			WHERE t3.valid_to + INTERVAL '0 23:59:59' DAY TO SECOND < t1.trans_date
			ORDER BY event_dt
		''')


def create_new_rows_to_report():
	cursor.execute('''
		CREATE TABLE de3hd.s_21_tmp_rp_frnd_nw as
			SELECT 
				t1.event_dt,
				t1.passport,
				t1.fio,
				t1.phone,
				t1.event_type,
				t1.report_dt
			FROM de3hd.s_21_rep_fraund_new t1
			LEFT JOIN de3hd.s_21_rep_fraund t2
			ON t1.event_dt = t2.event_dt AND t1.passport = t2.passport
			WHERE t2.event_dt is null
	''')

	cursor.execute('''
		INSERT INTO de3hd.s_21_rep_fraund (
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		) SELECT 
			event_dt,
			passport,
			fio,
			phone,
			event_type,
			report_dt
		FROM de3hd.s_21_tmp_rp_frnd_nw
	''')



def delete_tmp_report():
	cursor.execute('DROP TABLE de3hd.s_21_tmp_rp_frnd_nw')
	cursor.execute('DROP TABLE de3hd.s_21_rep_fraund_new')
	

def delete_table_STG():
	cursor.execute('DROP TABLE de3hd.s_21_trnsctns')
	cursor.execute('DROP TABLE de3hd.s_21_trmnls')
	cursor.execute('DROP TABLE de3hd.s_21_pssprt_blclst')


def delete_tmp_passport_bl():
	cursor.execute('DROP TABLE de3hd.s_21_pssprt_blclst_new')
	cursor.execute('DROP TABLE de3hd.s_21_stg_del_tmp_pssprt_bl')

#***********************************************************************************************


path_of_directory = 'C:/project/files_here/'

path_to_directory = 'C:/project/arhive/'


load_stg(path_of_directory)
