
# DB관련 클래스
class SQL():
    def __init__(self, host, user, password, db, port=3306, charset='utf8'):
        self.conn = pymysql.connect(host=host, user=user, password=password, db=db,
                                    port=port, charset=charset, cursorclass=pymysql.cursors.DictCursor)  # 접속정보

        self.cur = self.conn.cursor()  # 커서생성

    # Mysql 데이터 타입 체크
    def _queryDataTypeCheck(self, data) -> str:
        value = ''

        e = deepcopy(data)
        # 리스트 타입일 경우
        if type(data) is list:
            e = deepcopy(str(data).replace(', ', ','))  # 데이터 최적화(띄어쓰기 제거)
        # 딕셔너리 타입일 경우
        elif type(data) is dict:
            e = deepcopy(
                json.dumps(
                    data,
                    ensure_ascii=False,  # 한글 인코딩
                    separators=(',', ':')  # 데이터 최적화(띄어쓰기 제거)
                )
            )

        if IF.is_number(str(e)):
            value = e
        else:
            value = str(e)

        return value

    # 함수에 따라 리스트 생성
    def _functionTypeMakeList(self, function, data: list) -> list:
        try:
            data = list(data)
        except:
            return False

        return [function(value) for value in data]

    #### SQL(mysql & mariaDB)자동 완성 ####
    # SQL(mysql & mariaDB) Insert 쿼리 자동 완성
    def mySQLINSERTQuery(self, tableName: str, data: dict, showConsole=False):
        dataLen = len(data)
        if dataLen == 0:
            return (False, 'data가 비어있습니다.')

        # 기본 쿼리
        sqlQuery = 'INSERT into `'+tableName+'` (`{}`)values({})'

        # 컬럼명 가져오기
        key = '`, `'.join(list(data.keys()))
        # 값 가져오기
        # value = ','.join(str(e) if self._is_number(str(e)) else '"' + str(e)+'"' for e in data.values())
        try:
            value = self._functionTypeMakeList(
                self._queryDataTypeCheck, data.values()
            )
            if value == False:
                return (False, 'data가 dict가 아닙니다.')
        except:
            return (False, 'data가 dict가 아닙니다.')

        # 쿼리 완성
        sql = sqlQuery.format(key,  ', '.join(['%s'] * dataLen))

        # 해당 쿼리 시도
        returnDATA = self.returnDATA(sql, value, showConsole)

        # DB연결 끊킬시
        if not returnDATA['CHECK'] and "2006" in returnDATA['CODE']:
            self.mySQLINSERTQuery(tableName, data, showConsole)

        # 성공시 최종 반환
        return returnDATA

    # SQL(mysql & mariaDB) Select 쿼리 자동 완성
    def mySQLSELECTQuery(self, tableName: str, findData: str = None, showConsole=False):
        # 기본 쿼리
        if findData:
            sqlQuery = 'SELECT * from `'+tableName + \
                '` where {}'.format(findData)
        else:
            sqlQuery = 'SELECT * FROM `'+tableName+'`'

        # 해당 쿼리 시도
        returnDATA = self.returnDATA(sqlQuery, showConsole=showConsole)

        # 쿼리 성공시
        if returnDATA['CHECK']:
            returnDATA['DATA'] = self.cur.fetchall()

        # DB연결 끊킬시
        elif not returnDATA['CHECK'] and "2006" in returnDATA['CODE']:
            self.mySQLSELECTQuery(tableName, findData, showConsole)

        # 성공시 최종 반환
        return returnDATA

    # SQL(mysql & mariaDB) Update 쿼리 자동 완성
    def mySQLUPDATEQuery(self, tableName: str, upData: dict, findData: dict, showConsole=False):
        sql = 'UPDATE `'+tableName+'` set {} where {}'

        def dataMakeEqual(key):
            return f'`{key}` = %s'

        changes = ''
        if isinstance(upData, dict):
            changes = self._functionTypeMakeList(dataMakeEqual, upData.keys())
            changesData = self._functionTypeMakeList(
                self._queryDataTypeCheck, upData.values()
            )

        where = ''
        if isinstance(findData, dict):
            where = self._functionTypeMakeList(dataMakeEqual, findData.keys())
            whereData = self._functionTypeMakeList(
                self._queryDataTypeCheck, findData.values()
            )

        # upData와 findData가 모두 dict일 경우
        if isinstance(changes, list) and isinstance(where, list):
            sql = sql.format(', '.join(changes), ' and '.join(where))
            # 해당 쿼리 시도
            returnDATA = self.returnDATA(sql, changesData+whereData, showConsole)

        # upData만 dict일 경우
        elif isinstance(changes, list) and isinstance(findData, str):
            sql = sql.format(', '.join(changes), findData)
            sql = self.cur.mogrify(sql, changesData)
            # 해당 쿼리 시도
            returnDATA = self.returnDATA(sql, changesData, showConsole)

        # findData만 dict일 경우
        elif isinstance(where, list) and isinstance(upData, str):
            sql = sql.format(upData, ' and '.join(where))
            # 해당 쿼리 시도
            returnDATA = self.returnDATA(sql, whereData, showConsole)

        # 둘다 str일 경우
        else:
            sql = sql.format(upData, findData)
            # 해당 쿼리 시도
            returnDATA = self.returnDATA(sql, showConsole=showConsole)

        # DB연결 끊킬시
        if not returnDATA['CHECK'] and "2006" in returnDATA['CODE']:
            self.mySQLUPDATEQuery(tableName, upData, findData, showConsole)

        # 성공시 최종 반환
        return returnDATA
    ######################################

    # SQL쿼리 시도후 결과값 반환
    def returnDATA(self, sqlQuery, value=None, showConsole=False):
        returnDATA = {
            'CHECK': False,
            'CODE': '',
        }

        try:
            if value:
                sqlQuery = self.cur.mogrify(sqlQuery, value)  # sql query 생성
            self.cur.execute(sqlQuery)  # 커서로sql 실행
            if showConsole:
                returnDATA['TEXT_DEBUG'] = f'O (성공) : {sqlQuery}'

            self.conn.commit()  # 최종 저장
            returnDATA['CHECK'] = True

        except Exception as e:
            e = str(e)
            text = 'X (실패'

            if showConsole:
                if "for key 'PRIMARY'" in e:
                    text += f' 이미존재) : {sqlQuery}'
                    returnDATA['TEXT_WARNING'] = text
                    return returnDATA

                elif "Incorrect date value" in e:
                    try:
                        errorCode = e.split('.')[-1].split(' ')[0]
                        text += f'{errorCode}값이 없습니다.) : ERROR -> {e}\n\t\t\t{sqlQuery}'
                    except:
                        text += f' 값이 없습니다.) : ERROR -> {e}\n\t\t\t{sqlQuery}'
                else:
                    text += f' ERROR) :  -> {e}\n\t\t\t{sqlQuery}'
                returnDATA['TEXT_ERROR'] = text

            if "2006" in e:
                self.conn.ping(reconnect=True)  # try reconnect
                returnDATA['CODE'] = f"2006 : 재열결 시도함... -> {e}"

        return returnDATA
