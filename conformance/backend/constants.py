from pm4py.util.constants import CASE_CONCEPT_NAME
from pm4py.util import xes_constants

ACTIVITY_NAME = xes_constants.DEFAULT_NAME_KEY
TIMESTAMP_NAME = xes_constants.DEFAULT_TIMESTAMP_KEY
START_TIMESTAMP_NAME = xes_constants.DEFAULT_START_TIMESTAMP_KEY
START_MARK, END_MARK = "@@startnode", "@@endnode"
ORIGINAL_ACTIVITY_NAME = "original_concept:name"
ORIGINAL_ACTIVITY = "original_movimentoID"
TIME_FORMATTED_NAME = "time:formatted"
ACTIVITY_ID_NAME = "movimentoID"
USER_KEY = "usuarioID"
NAME_KEY = "nomeServidor"
TYPE_KEY = "tipoServidor"
DURATION_NAME = "duration"
CLASSE_NAME = "classe"
JULGAMENTO_COM_RES = 385
JULGAMENTO_SEM_RES = 218
DEFINITIVO = 246
# Movimentos de suspensão ou sobrestamento, são os dois movimentos que são apenas 1 nível abaixo 
# de despacho ou decisão.
SUSPENSOES_LIST = [25, 11025]
