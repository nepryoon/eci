package eci.authz

default decision := {"allow": false, "reason": "policy_denied"}

data_actions := {
    "/eci.retrieval.v1.RetrievalEngine/HybridSearch",
    "/eci.retrieval.v1.RetrievalEngine/ImpactAnalysis",
    "/eci.retrieval.v1.RetrievalEngine/GetNode",
    "/eci.retrieval.v1.RetrievalEngine/ExpandNeighbors",
    "/eci.semanticcache.v1.SemanticCache/Get",
    "/eci.semanticcache.v1.SemanticCache/Put",
}

health_actions := {
    "/eci.retrieval.v1.RetrievalEngine/Health",
}

subject := object.get(input, "subject", {})
tenant_id := object.get(subject, "tenant_id", "")
user_id := object.get(subject, "user_id", "")
allowed_repos := object.get(subject, "allowed_repos", [])
acl_groups := object.get(subject, "acl_groups", [])
action := object.get(input, "action", "")

valid_repo_scope if {
    count(allowed_repos) > 0
    every repo in allowed_repos {
        is_string(repo)
        trim(repo, " \t\r\n") != ""
    }
}

valid_acl_scope if {
    count(acl_groups) > 0
    every group in acl_groups {
        is_string(group)
        trim(group, " \t\r\n") != ""
    }
}

decision := {"allow": false, "reason": "missing_tenant"} if {
    trim(tenant_id, " \t\r\n") == ""
} else := {"allow": false, "reason": "missing_user"} if {
    trim(user_id, " \t\r\n") == ""
} else := {"allow": false, "reason": "unknown_action"} if {
    not action in data_actions
    not action in health_actions
} else := {"allow": true, "reason": "allow"} if {
    action in health_actions
} else := {"allow": false, "reason": "empty_repo_scope"} if {
    action in data_actions
    not valid_repo_scope
} else := {"allow": false, "reason": "empty_acl_scope"} if {
    action in data_actions
    not valid_acl_scope
} else := {"allow": true, "reason": "allow"} if {
    action in data_actions
    valid_repo_scope
    valid_acl_scope
}
