from app.services.persistence_health import persistence_health

def get_liveliness():
    return{
        "status" : "healthy"
    }

def get_readiness():
    persistence_state = persistence_health.get_state().value

    if persistence_state == "unhealthy":
        return {
            "status": "not_Ready",
            "persistence": persistence_state
        }

    return {
        "status": "ready",
        "persistence": persistence_state
    }