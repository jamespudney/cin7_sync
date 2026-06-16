"""My Profile page."""

from __future__ import annotations

import os

import pandas as pd
import streamlit as st


SLACK_OAUTH_ENV_VARS = (
    "SLACK_OAUTH_CLIENT_ID",
    "SLACK_OAUTH_CLIENT_SECRET",
    "SLACK_OAUTH_REDIRECT_URI",
    "SLACK_USER_TOKEN_ENCRYPTION_KEY",
)


def missing_slack_oauth_env_vars() -> list[str]:
    return [name for name in SLACK_OAUTH_ENV_VARS
            if not os.environ.get(name, "").strip()]


def render_my_profile(*, current_user_profile, page_options, db_module) -> None:
    db = db_module
    st.header("👤 My Profile")
    st.caption(
        "Your profile is loaded automatically when you sign in. "
        "Edit your role, default landing page, or email below. "
        "Admins can manage other users via the panel at the bottom.")

    me = current_user_profile or {}
    is_admin = (me.get("role") == "admin")
    me_is_super = db.is_super_admin(
        me.get("display_name") or "", me.get("role") or "")

    if not me:
        st.warning(
            "No profile loaded. Sign out and sign back in via the "
            "sidebar.")
    else:
        with st.form("_my_profile_form", clear_on_submit=False):
            st.markdown(f"**Display name:** `{me.get('display_name')}` "
                        f"(set at sign-in; cannot be edited here — "
                        f"sign out and back in with a different name "
                        f"to switch identity)")
            new_role = me.get("role") or db.DEFAULT_NEW_USER_ROLE
            st.markdown(
                f"**Role:** `{new_role}` "
                + ("— you're a super-admin; change roles on the "
                   "User Permissions page."
                   if me_is_super
                   else "— role changes are made by a "
                        "super-admin on the User Permissions "
                        "page."))
            new_email = st.text_input(
                "Email (optional)",
                value=me.get("email") or "",
                placeholder="you@w4susa.com")
            new_default_page = st.selectbox(
                "Default landing page",
                options=["(none)"] + list(page_options),
                index=(list(page_options).index(me.get("default_page")) + 1
                       if me.get("default_page") in page_options
                       else 0),
                help="When you sign in, jump straight to this page "
                     "instead of Overview.")
            save_profile = st.form_submit_button(
                "💾 Save profile",
                type="primary", width="stretch")

        if save_profile:
            try:
                final_default = (None
                                 if new_default_page == "(none)"
                                 else new_default_page)
                db.upsert_user(
                    display_name=me.get("display_name"),
                    role=new_role,
                    email=new_email.strip() or None,
                    active=True,
                    default_page=final_default,
                    actor=me.get("display_name"))
                refreshed = db.get_user_by_name(me.get("display_name"))
                if refreshed is not None:
                    st.session_state["current_user_profile"] = {
                        "user_id": int(refreshed["user_id"]),
                        "display_name": refreshed["display_name"],
                        "role": refreshed["role"],
                        "email": refreshed["email"],
                        "default_page": refreshed["default_page"],
                        "active": bool(refreshed["active"]),
                    }
                st.success(":white_check_mark: Profile saved.")
                st.rerun()
            except Exception as exc:  # noqa: BLE001
                st.error(f"Save failed: {exc}")

    if me.get("user_id"):
        missing_oauth_env = missing_slack_oauth_env_vars()
        if missing_oauth_env:
            if is_admin or me_is_super:
                st.divider()
                with st.expander(
                        "Slack OAuth for Viktor is not configured",
                        expanded=False):
                    st.info(
                        "This optional setup lets the dashboard post "
                        "marketing questions to Slack as the signed-in "
                        "user. The normal Slack bot still works without it.")
                    st.caption(
                        "Set these Render environment variables to enable "
                        "the Connect Slack button:")
                    st.code("\n".join(missing_oauth_env), language="text")
        else:
            st.divider()
            st.subheader("📡 Connect Slack (for Viktor)")
            st.caption(
                "Authorise the dashboard to ask Viktor on your behalf "
                "when you ask marketing questions here. One-time "
                "OAuth — no passwords stored, just a scoped Slack "
                "token (encrypted at rest). Disconnect any time below.")
            try:
                import slack_oauth as slack_oauth_ui
                connected = slack_oauth_ui.is_user_connected(me["user_id"])
            except Exception as exc:  # noqa: BLE001
                connected = False
                st.warning(f"Slack OAuth module not configured: {exc}")
            if connected:
                try:
                    slack_uid = slack_oauth_ui.get_user_slack_id(me["user_id"])
                except Exception:  # noqa: BLE001
                    slack_uid = None
                st.success(
                    f":white_check_mark: Connected"
                    f"{f' as `<@{slack_uid}>`' if slack_uid else ''}. "
                    f"Marketing questions in the AI Assistant will be "
                    f"forwarded to Viktor on your behalf.")
                if st.button(
                        ":electric_plug: Disconnect Slack",
                        key="_disconnect_slack",
                        help="Revoke the dashboard's permission to post "
                             "as you. You can reconnect any time."):
                    try:
                        slack_oauth_ui.disconnect_user(me["user_id"])
                        st.success("Disconnected.")
                        st.rerun()
                    except Exception as exc:  # noqa: BLE001
                        st.error(f"Disconnect failed: {exc}")
            else:
                try:
                    import secrets as secrets_module
                    state = secrets_module.token_urlsafe(24)
                    st.session_state["_slack_oauth_state"] = state
                    auth_url = slack_oauth_ui.build_authorize_url(state)
                    st.markdown(
                        f"[🔗 **Connect Slack** "
                        f"(opens Slack to authorise)]({auth_url})")
                    st.caption(
                        "Required Slack-app config (one-time, see "
                        "slack_oauth.py docstring): User Token Scopes "
                        "must include `chat:write`, and the Redirect "
                        "URL must match this dashboard's URL.")
                except Exception as exc:  # noqa: BLE001
                    st.error(f"Connect button unavailable: {exc}.")

    if is_admin:
        st.divider()
        st.subheader(":shield: Admin — all users")
        try:
            all_users = db.list_users(active_only=False)
        except Exception as exc:  # noqa: BLE001
            all_users = []
            st.error(f"Could not load users: {exc}")
        if all_users:
            st.dataframe(
                pd.DataFrame([
                    {
                        "user_id": u["user_id"],
                        "display_name": u["display_name"],
                        "role": u["role"],
                        "email": u["email"] or "",
                        "active": bool(u["active"]),
                        "default_page": u["default_page"] or "",
                        "created_at": str(u["created_at"]),
                        "updated_at": str(u["updated_at"]),
                    } for u in all_users
                ]),
                width="stretch", height=300)
            st.caption(
                "To edit another user, ask them to sign in and "
                "update their own profile, OR add a per-user "
                "edit form here in a follow-up version. "
                "Deactivating users is also future work.")
