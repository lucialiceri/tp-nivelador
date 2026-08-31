import socket
import logger
import safe_socket
import lottery

class Server:
    def __init__(self, server_host: str, server_port: int, output_file: str) -> None:
        self.server_host = server_host
        self.server_port = server_port
        self.output_file = output_file

    def _handle_client(self, client_socket):
        action = "handle-client"
        message_amount = 0
        agency_id = None
        lottery_instance = lottery.Lottery(self.output_file)
        try:
            logger.info(action, logger.LogResult.in_progress)
            while True:
                line_size = safe_socket.recv_all(client_socket, 4)
                size = int.from_bytes(line_size, "big")
                if size == 0:
                    winners = self._get_winners(lottery_instance, agency_id)
                    logger.info(action, logger.LogResult.success, "winners-amount", len(winners))

                    response = self._serialize_winners(winners)
                    response_size = len(response).to_bytes(4, "big")

                    safe_socket.send_all(client_socket, response_size)
                    safe_socket.send_all(client_socket, response)
                    logger.info(action, logger.LogResult.success, "response-sent")
                    return

                client_message = safe_socket.recv_all(client_socket, size)
                logger.info(action, logger.LogResult.in_progress, "message", client_message)

                client_message = client_message.decode("utf-8")
                bets = client_message.split("\n")
                for bet in bets:
                    self._store_bet(lottery_instance, bet)

                if agency_id is None:
                    agency_id = bet.agency_id

                message_amount += 1    

        except ConnectionError:
            logger.error(
                action, logger.LogResult.success, "messages-amount", message_amount
            )
        except Exception as e:
            logger.error(
                action, logger.LogResult.fail, "messages-amount", message_amount
            )
            raise e

    def _get_winners(self, lottery_instance, agency_id):
        if agency_id is None:
            return []

        winners = []
        for bet in lottery_instance.load_bets():
            if lottery_instance.has_won(bet) and bet.agency_id == agency_id:
                winners.append(bet)

        return winners

    def _serialize_winners(self, winners):
        if not winners:
            return b""

        lines = []
        for bet in winners:
            lines.append(f"{bet.first_name},{bet.last_name},{bet.document},{bet.birthdate},{bet.number}")

        return "\n".join(lines).encode("utf-8")

    def _store_bet(self, lottery_instance, bet_message):
        
        row = bet_message.split(",")

        if len(row) != 6:
            raise ValueError("Invalid bet message format")

        agency_id = int(row[0])
        first_name = row[1]
        last_name = row[2]
        document = int(row[3])
        birthdate = row[4]
        number = int(row[5])
        
        bet = lottery.Bet(agency_id, first_name, last_name, document, birthdate, number)

        lottery_instance.store_bets([bet])
        
        return bet

    def run(self):
        action = "accept-connection"
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
            server_socket.bind((self.server_host, self.server_port))
            server_socket.listen()
            while True:
                try:
                    logger.info(action, logger.LogResult.in_progress)
                    client_socket, _ = server_socket.accept()
                except Exception as e:
                    logger.error(action, logger.LogResult.fail)
                    raise e
                logger.info(action, logger.LogResult.success)

                self._handle_client(client_socket)
