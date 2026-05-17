"""Event definitions + index-to-event wire-up map.

The events module is the **design-intent** pattern for VideoDB usage:
events are created ONCE on the connection and reused across both streams.
The bootstrap script reads ``EVENT_DEFINITIONS`` to materialise events,
then walks ``INDEX_EVENT_MAP`` to attach an alert per (stream, index, event)
triple. With 2 streams x 18 wire-ups = 36 alerts total.

Tiers:
    1 — informational (green emoji in Telegram)
    2 — notable (yellow)
    3 — urgent / threat (red)
"""

from __future__ import annotations

from typing import Literal, TypedDict


class EventDefinition(TypedDict):
    """One row in EVENT_DEFINITIONS — typed so a typo in any literal becomes
    a static error rather than a runtime KeyError at bootstrap time."""

    id_var: str
    label: str
    tier: Literal[1, 2, 3]
    prompt: str


EVENT_DEFINITIONS: list[EventDefinition] = [
    # ──── Species index events ────
    {
        "id_var": "rare_species",
        "label": "rare_species_sighting",
        "tier": 1,
        "prompt": (
            "Detect when a rare or flagship species is reported: leopard, rhino, "
            "wild dog, cheetah, pangolin, or any species marked as IUCN endangered."
        ),
    },
    {
        "id_var": "mixed_aggregation",
        "label": "mixed_aggregation",
        "tier": 1,
        "prompt": (
            "Detect when scene_state is mixed_species and total animals is greater "
            "than 5 — multi-species aggregation event."
        ),
    },
    {
        "id_var": "juvenile_present",
        "label": "juvenile_present",
        "tier": 1,
        "prompt": (
            "Detect when any reported animal has age_sex containing 'juvenile', "
            "'calf', 'cub', 'chick', or 'foal'."
        ),
    },
    {
        "id_var": "large_aggregation",
        "label": "large_aggregation",
        "tier": 2,
        "prompt": ("Detect when total animals visible is 15 or more — large aggregation event."),
    },
    # ──── Behavior index events ────
    {
        "id_var": "predator_activity",
        "label": "predator_activity",
        "tier": 2,
        "prompt": (
            "Detect when behavior includes any of: fleeing, alarm_response, chasing, "
            "threat_display, frozen — or when interaction type is predator_prey."
        ),
    },
    {
        "id_var": "parental_care",
        "label": "parental_care",
        "tier": 1,
        "prompt": (
            "Detect when behavior contains nursing, feeding_young, leading_young, "
            "defending_young, or interaction type is parent_offspring."
        ),
    },
    {
        "id_var": "welfare_concern",
        "label": "welfare_concern",
        "tier": 3,
        "prompt": (
            "Detect when an animal is reported with limping, apparent injury, "
            "abnormal posture, or isolation_from_group in the anomaly field."
        ),
    },
    {
        "id_var": "notable_social",
        "label": "notable_social_behavior",
        "tier": 1,
        "prompt": "Detect courtship_display, mating, or play behavior.",
    },
    # ──── Environment index events ────
    {
        "id_var": "mortality_event",
        "label": "mortality_event",
        "tier": 3,
        "prompt": ("Detect when flags contain carcass_or_remains_visible or recent_kill_evidence."),
    },
    {
        "id_var": "human_intrusion_visual",
        "label": "potential_human_intrusion_visual",
        "tier": 3,
        "prompt": (
            "Detect when flags contain human_made_object_visible AND the time is night, "
            "or when human-made objects are flagged in a context where they should not "
            "be present."
        ),
    },
    {
        "id_var": "camera_health",
        "label": "camera_health_issue",
        "tier": 1,
        "prompt": "Detect when flags contain camera_obstruction or camera_failure.",
    },
    {
        "id_var": "water_critical",
        "label": "water_critical",
        "tier": 2,
        "prompt": (
            "Detect when water state is very_low_drying or dry — flag for drought monitoring."
        ),
    },
    # ──── Audio index events ────
    {
        "id_var": "gunshot",
        "label": "POACHING_ALERT_GUNSHOT",
        "tier": 3,
        "prompt": (
            "Detect when any sound is classified as gunshot in anthropophony, "
            "with confidence medium or high."
        ),
    },
    {
        "id_var": "chainsaw",
        "label": "ILLEGAL_LOGGING_ALERT",
        "tier": 3,
        "prompt": (
            "Detect when any sound is classified as chainsaw with confidence medium or high."
        ),
    },
    {
        "id_var": "human_intrusion_audio",
        "label": "human_intrusion_audio",
        "tier": 3,
        "prompt": (
            "Detect when anthropophony sounds (vehicle_engine, motorcycle, voices_human) "
            "are reported with intensity clear or loud, at any time, or with any "
            "intensity during night hours."
        ),
    },
    {
        "id_var": "alarm_call",
        "label": "alarm_call_detected",
        "tier": 2,
        "prompt": (
            "Detect when SIGNAL contains ALARM_CALL or when any alarm_call sound type "
            "is reported with confidence medium or high."
        ),
    },
    {
        "id_var": "predator_vocal",
        "label": "predator_vocalization",
        "tier": 2,
        "prompt": (
            "Detect when SIGNAL contains PREDATOR_VOCALIZATION, or when sound type "
            "contains lion_roar, leopard_sawing, hyena_whoop, or any term tagged as "
            "a predator vocalization."
        ),
    },
    {
        "id_var": "acoustic_silence",
        "label": "acoustic_anomaly_silence",
        "tier": 2,
        "prompt": "Detect when SIGNAL contains ABNORMAL_SILENCE.",
    },
]

# Wire-up matrix: which events attach to which index kind.
# Bootstrap walks this map and calls index.create_alert() per (stream, kind, event_id).
INDEX_EVENT_MAP: dict[str, list[str]] = {
    "species": ["rare_species", "mixed_aggregation", "juvenile_present", "large_aggregation"],
    "behavior": ["predator_activity", "parental_care", "welfare_concern", "notable_social"],
    "environment": ["mortality_event", "human_intrusion_visual", "camera_health", "water_critical"],
    "audio": [
        "gunshot",
        "chainsaw",
        "human_intrusion_audio",
        "alarm_call",
        "predator_vocal",
        "acoustic_silence",
    ],
}
