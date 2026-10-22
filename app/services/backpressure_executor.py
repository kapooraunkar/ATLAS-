from app.services.backpressure_policy import BackpressureAction
from app.services.telemetry_queue import AdmissionMode, set_admission_mode
class BackpressureExecutor:

    def execute(self, action: BackpressureAction):

        if action ==  BackpressureAction.NONE:
            set_admission_mode(AdmissionMode.ACCEPT_ALL)

        if action ==  BackpressureAction.SHED_NORMAL:
            set_admission_mode(AdmissionMode.SHED_NORMAL)

        if action == BackpressureAction.SHED_NON_CRITICAL:
            set_admission_mode(AdmissionMode.SHED_NON_CRITICAL)

        if action == BackpressureAction.EMERGENCY:
            set_admission_mode(AdmissionMode.SHED_NON_CRITICAL)

backpressure_executor = BackpressureExecutor()