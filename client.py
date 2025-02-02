from edalite.core import EdaliteCaller

caller = EdaliteCaller(debug=True).connect()

# 즉시 실행 함수 호출
result = caller.request("example.immediate", "Hello!")
print("Immediate result:", result)

# 지연 실행 함수 호출
task_id = caller.delay("example.deferred", "Hello, deferred!")
print("Deferred task id:", task_id)

# 결과 조회 (잠시 후)
import time
time.sleep(3)
deferred_result = caller.get_deferred_result("example.deferred", task_id)
print("Deferred result:", deferred_result)

caller.close()
