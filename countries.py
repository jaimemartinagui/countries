"""
    Countries.
"""

import json
import time
import random
import datetime
import multiprocessing

import regex
import requests
from unidecode import unidecode

from decorators import try_n_times
from database_connection import DatabaseConnection
from config.config import DATA_PATH, PLAYERS, INIT_TIME_LIMIT, NUMBER_OF_QUESTIONS, \
                          RESPONSE_TIME_LIMIT, N_PAR, TOKENS, CHAT_IDS

class Countries():
    """Clase con la implementacion del juego."""

    def __init__(self, data_path, players, init_time_limit, number_of_questions, response_time_limit, 
                 n_par, tokens, chat_ids):

        self.data_path           = data_path
        self.players             = players
        self.init_time_limit     = init_time_limit
        self.number_of_questions = number_of_questions
        self.response_time_limit = response_time_limit
        self.n_par               = n_par
        self.tokens              = tokens
        self.chat_ids            = chat_ids
        random.shuffle(self.players)

    def run_countries(self, second_turn=False):
        """Metodo que ejecuta la app y se asegura de que se cierre la conexion con la base de datos."""

        try:
            self._run_countries(second_turn)
        finally:
            if hasattr(self, 'database_conn'):
                self.database_conn.close_connection()

    def _run_countries(self, second_turn):
        """Metodo principal que ejecuta la aplicacion."""

        countries        = self._load_data()
        countries_sample = [(random.randint(0, 3), country) for country in random.sample(countries, self.number_of_questions)]
        players          = self._get_second_turn_players() if second_turn else self.players
        args             = [(countries_sample, player, second_turn) for player in players]
        pool             = multiprocessing.Pool(processes=self.n_par)
        results          = pool.starmap(self._player_execution, args)

        self.database_conn = DatabaseConnection()
        self.cur           = self.database_conn.get_cursor()

        self._add_players_to_database()
        self._update_player_points(results)
        self._generate_table_backup()
        self._update_second_turn_table(second_turn, results)
        if second_turn:
            self._send_closing_message()

    def _player_execution(self, countries_sample, player, second_turn):
        """Metodo para ejecutar el juego para un unico jugador y poder paralelizarlo."""

        token   = self.tokens[player]
        chat_id = self.chat_ids[player]
        self._send_welcome_message(token, chat_id, player)

        if self._wait_for_response(token, self.init_time_limit) is None:
            msg = "Ha perdido ambos turnos. Hasta ma??ana!" if second_turn else "Ha perdido el primer turno. Hasta luego!"
            self._send_telegram_msg(token, chat_id, msg)
            return player, None

        daily_points  = 0

        for i, (question, country) in enumerate(countries_sample):

            if question < 3:
                self._send_telegram_msg(token, chat_id, f"{i + 1}. Capital de {country['Country']}?")
                answer = country['Capital']
            else:
                self._send_telegram_msg(token, chat_id, f"{i + 1}. Pa??s cuya capital es {country['Capital']}?")
                answer = country['Country']

            response = self._wait_for_response(token, self.response_time_limit)
            if response is None:
                self._send_telegram_msg(token, chat_id, "Se acab?? el tiempo. Conteste m??s r??pido la pr??xima vez.")
            else:
                daily_points = self._check_country_response(token, chat_id, response, country, answer, daily_points)

            self._send_telegram_msg(token, chat_id, "Continente?")

            response = self._wait_for_response(token, self.response_time_limit)
            if response is None:
                self._send_telegram_msg(token, chat_id, "Se acab?? el tiempo. Conteste m??s r??pido la pr??xima vez.")
            else:
                daily_points = self._check_continent_response(token, chat_id, response, country, country['Continent'], daily_points)

        self._send_telegram_msg(token, chat_id, f"FIN! Hoy ha sumado {daily_points} puntos.")

        return player, daily_points

    def _load_data(self):
        """Metodo para cargar el fichero json de datos."""

        with open(self.data_path) as f:
            countries = json.load(f)

        return countries

    @try_n_times(n=10)
    def _send_telegram_msg(self, token, chat_id, message):
        """Metodo para enviar un mensaje a un chat de Telegram."""

        return requests.post(f'https://api.telegram.org/bot{token}/sendMessage?chat_id={chat_id}&text={message}')

    @try_n_times(n=10)
    def _get_telegram_updates(self, token):
        """Metodo para obtener los updates de un chat de Telegram."""

        return requests.post(f'https://api.telegram.org/bot{token}/getUpdates')

    def _wait_for_response(self, token, time_limit):
        """Metodo que espera a la respuesta con el limite de tiempo determinado."""

        init_time = time.time()
        response  = None

        while response is None and time.time() - init_time <= time_limit:
            api_response = self._get_telegram_updates(token).json()
            if updates := api_response['result']:
                valid_updates = [update for update in updates if update['message']['date'] >= init_time]
                if valid_updates:
                    response = valid_updates[-1]
            time.sleep(0.5)

        return response

    def _send_welcome_message(self, token, chat_id, player):
        """Metodo para enviar el mensaje inicial."""

        self._send_telegram_msg(token, chat_id, 
            f"=========================\nCOUNTRIES ({datetime.datetime.today().strftime('%d-%m-%Y')})\n=========================\n\n" + 
            f"Hola {player}, bienvenido de nuevo a Countries!\n" + 
            f"Responda a este mensaje para comenzar.\n" + 
            f"Cualquier respuesta iniciar?? el juego.\n" + 
            f"Si en {self.init_time_limit} segundos no responde perder?? su turno de hoy."
        )

    def _check_country_response(self, token, chat_id, response, country, answer, points):
        """Metodo que comprueba si la respuesta a la pregunta de la capital es correcta."""

        unicode_answer   = unidecode(f"{answer.lower()}")
        unicode_response = unidecode(f"{response['message']['text'].lower()}")

        if regex.match(f'({unicode_answer}){{e<=1}}$', unicode_response):
            self._send_telegram_msg(token, chat_id, "Correcto! Ha ganado 3 puntos.")
            points += 3
        else:
            self._send_telegram_msg(token, chat_id, f'Incorrecto. La capital de {country["Country"]} es {country["Capital"]}.')

        return points

    def _check_continent_response(self, token, chat_id, response, country, answer, points):
        """Metodo que comprueba si la respuesta a la pregunta de la capital es correcta."""

        unicode_answer   = unidecode(f"{answer.lower()}")
        unicode_response = unidecode(f"{response['message']['text'].lower()}")

        if regex.match(f'({unicode_answer}){{e<=1}}$', unicode_response):
            self._send_telegram_msg(token, chat_id, "Correcto! Ha ganado 1 punto.")
            points += 1
        else:
            self._send_telegram_msg(token, chat_id, f'Incorrecto. {country["Country"]} est?? en {country["Continent"]}.')

        return points

    def _get_second_turn_players(self):
        """Metodo para obtener la lista de jugadores para la ejecucion del segundo turno."""

        tmp_database_conn = DatabaseConnection()
        tmp_cur           = tmp_database_conn.get_cursor()
        tmp_cur.execute("SELECT jugador FROM second_turn;")
        players_list = [player_tuple[0] for player_tuple in tmp_cur.fetchall()]
        tmp_database_conn.close_connection()

        return players_list

    def _add_players_to_database(self):
        """Metodo para incluir los jugadores que falten en la tabla de clasificacion."""

        self.cur.execute("SELECT jugador FROM classification")
        players_in_table = [item[0] for item in self.cur.fetchall()]

        values_to_insert = ""
        for player in self.players:
            if player not in players_in_table:
                values_to_insert += f"('{player}', 0), "

        if values_to_insert:
            self.cur.execute(f"INSERT INTO classification (jugador, puntos) VALUES {values_to_insert[:-2]};")

    def _update_player_points(self, results):
        """Metodo para actualizar los puntos de todos los jugadores."""

        for player, points in filter(lambda x: x[1] is not None, results):
            self.cur.execute(f"SELECT puntos FROM classification WHERE jugador = '{player}'")
            self.cur.execute(f"UPDATE classification SET puntos = '{self.cur.fetchone()[0] + points}' WHERE jugador = '{player}'")

    def _generate_table_backup(self):
        """Metodo para crear un backup de la tabla de clasificacion."""

        self.cur.execute("DROP TABLE IF EXISTS classification_backup;")
        self.cur.execute("CREATE TABLE classification_backup AS (SELECT * FROM classification WHERE 1=1);")

    def _update_second_turn_table(self, second_turn, results):
        """Metodo para actualizar la tabla que contiene los jugadores para la segunda ejecucion."""

        if second_turn:
            self.cur.execute("DELETE FROM second_turn WHERE 1=1;")
        elif players_who_skipped_first_turn := self._get_players_who_skipped_first_turn(results):
            values_to_insert = ""
            for player in players_who_skipped_first_turn:
                values_to_insert += f"('{player}'), "
            self.cur.execute(f"INSERT INTO second_turn (jugador) VALUES {values_to_insert[:-2]};")

    def _get_players_who_skipped_first_turn(self, results):
        """Metodo que devuelve los jugadores que se han saltado el primer turno."""

        return [result[0] for result in filter(lambda x: x[1] is None, results)]

    def _send_closing_message(self):
        """Metodo para enviar el mensaje con la clasificacion."""

        classification     = self._get_classification()
        classification_msg = self._get_classification_msg(classification)
        for player in self.players:
            self._send_telegram_msg(self.tokens[player], self.chat_ids[player], 
                f'CLASIFICACI??N\n-----------------------------\n{classification_msg[:-1]}\n-----------------------------\n\n' + 
                f'{classification[0][0]} va en cabeza.\n{classification[-1][0]} ponte las pilas...\n\n' + 
                f'Hasta mana??a!\n\n' + 
                f"========================="
            )

    def _get_classification(self):
        """Metodo para obtener la clasificacion."""

        self.cur.execute("SELECT jugador, puntos FROM classification ORDER BY puntos DESC;")

        return self.cur.fetchall()

    def _get_classification_msg(self, classification):
        """Metodo para obtener el mensaje de la clasificacion para Telegram."""

        return "".join(f'{i + 1}. {player} --> {points} puntos\n' for i, (player, points) in enumerate(classification))

if __name__ == '__main__':

    import sys

    SECOND_TURN = bool(sys.argv[1]) if len(sys.argv) > 1 else False

    countries = Countries(
        DATA_PATH, PLAYERS, INIT_TIME_LIMIT, NUMBER_OF_QUESTIONS, 
        RESPONSE_TIME_LIMIT, N_PAR, TOKENS, CHAT_IDS
    )

    countries.run_countries(second_turn=SECOND_TURN)
