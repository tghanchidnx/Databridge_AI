import React, { useState, useEffect } from "react";
import { useNavigate, useSearchParams } from "react-router-dom";
import {
  snowflakeAPI,
  setAuthToken,
  setUserData,
  type SnowflakeSSOLoginRequest,
} from "../../lib/snowflake-api";

interface SnowflakeLoginProps {
  onSuccess?: (authResponse: any) => void;
  redirectUri?: string;
}

export const SnowflakeLogin: React.FC<SnowflakeLoginProps> = ({
  onSuccess,
  redirectUri = "http://localhost:5000/auth/snowflake/callback",
}) => {
  const navigate = useNavigate();
  const [searchParams] = useSearchParams();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [snowflakeAccount, setSnowflakeAccount] = useState("");

  // Handle OAuth callback
  useEffect(() => {
    const code = searchParams.get("code");
    const error = searchParams.get("error");

    if (error) {
      setError(`Authentication failed: ${error}`);
      return;
    }

    if (code) {
      handleCallback(code);
    }
  }, [searchParams]);

  const handleCallback = async (code: string) => {
    setLoading(true);
    setError(null);

    try {
      const response = await snowflakeAPI.ssoCallback({
        code,
        snowflakeAccount:
          localStorage.getItem("snowflake_account") || undefined,
      });

      // Store auth token and user data
      setAuthToken(response.token);
      setUserData(response.user);

      // Clear stored account
      localStorage.removeItem("snowflake_account");

      if (onSuccess) {
        onSuccess(response);
      } else {
        navigate("/dashboard");
      }
    } catch (err: any) {
      setError(err.message || "Failed to authenticate with Snowflake");
    } finally {
      setLoading(false);
    }
  };

  const handleSnowflakeLogin = async () => {
    if (!snowflakeAccount.trim()) {
      setError("Please enter your Snowflake account identifier");
      return;
    }

    setLoading(true);
    setError(null);

    try {
      // Store account for callback
      localStorage.setItem("snowflake_account", snowflakeAccount);

      // Get authorization URL
      const response = await snowflakeAPI.initSSO({ snowflakeAccount });

      // Redirect to Snowflake OAuth
      window.location.href = response.authorizationUrl;
    } catch (err: any) {
      setError(err.message || "Failed to initiate Snowflake login");
      setLoading(false);
    }
  };

  return (
    <div className="snowflake-login-container">
      <div className="snowflake-login-card">
        <div className="snowflake-login-header">
          <svg
            className="snowflake-logo"
            width="48"
            height="48"
            viewBox="0 0 24 24"
            fill="none"
            xmlns="http://www.w3.org/2000/svg"
          >
            <path
              d="M12 2L14.5 7.5L20 10L14.5 12.5L12 18L9.5 12.5L4 10L9.5 7.5L12 2Z"
              fill="#29B5E8"
            />
            <path
              d="M12 6L13.5 9.5L17 11L13.5 12.5L12 16L10.5 12.5L7 11L10.5 9.5L12 6Z"
              fill="#FFFFFF"
            />
          </svg>
          <h2>Sign in with Snowflake</h2>
          <p>Connect to your Snowflake account using SSO</p>
        </div>

        {error && (
          <div className="alert alert-error">
            <svg
              xmlns="http://www.w3.org/2000/svg"
              className="alert-icon"
              fill="none"
              viewBox="0 0 24 24"
              stroke="currentColor"
            >
              <path
                strokeLinecap="round"
                strokeLinejoin="round"
                strokeWidth={2}
                d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z"
              />
            </svg>
            <span>{error}</span>
          </div>
        )}

        <div className="snowflake-login-form">
          <div className="form-group">
            <label htmlFor="snowflakeAccount">
              Snowflake Account Identifier
            </label>
            <input
              id="snowflakeAccount"
              type="text"
              className="form-input"
              placeholder="e.g., zf07542.south-central-us.azure"
              value={snowflakeAccount}
              onChange={(e) => setSnowflakeAccount(e.target.value)}
              disabled={loading}
            />
            <small className="form-help">
              Your account identifier can be found in your Snowflake URL
            </small>
          </div>

          <button
            className="btn btn-primary btn-block"
            onClick={handleSnowflakeLogin}
            disabled={loading || !snowflakeAccount.trim()}
          >
            {loading ? (
              <>
                <svg className="spinner" viewBox="0 0 24 24">
                  <circle
                    className="spinner-circle"
                    cx="12"
                    cy="12"
                    r="10"
                    stroke="currentColor"
                    strokeWidth="4"
                    fill="none"
                  />
                </svg>
                Connecting...
              </>
            ) : (
              <>
                <svg
                  xmlns="http://www.w3.org/2000/svg"
                  className="btn-icon"
                  fill="none"
                  viewBox="0 0 24 24"
                  stroke="currentColor"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M14 5l7 7m0 0l-7 7m7-7H3"
                  />
                </svg>
                Continue with Snowflake
              </>
            )}
          </button>
        </div>

        <div className="snowflake-login-footer">
          <p>
            Don't have a Snowflake account?{" "}
            <a
              href="https://signup.snowflake.com/"
              target="_blank"
              rel="noopener noreferrer"
            >
              Sign up
            </a>
          </p>
        </div>
      </div>

      <style>{`
        .snowflake-login-container {
          display: flex;
          justify-content: center;
          align-items: center;
          min-height: 100vh;
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          padding: 20px;
        }

        .snowflake-login-card {
          background: white;
          border-radius: 16px;
          box-shadow: 0 20px 60px rgba(0, 0, 0, 0.3);
          padding: 40px;
          max-width: 480px;
          width: 100%;
        }

        .snowflake-login-header {
          text-align: center;
          margin-bottom: 32px;
        }

        .snowflake-logo {
          margin-bottom: 16px;
        }

        .snowflake-login-header h2 {
          font-size: 24px;
          font-weight: 700;
          color: #1a202c;
          margin-bottom: 8px;
        }

        .snowflake-login-header p {
          font-size: 14px;
          color: #718096;
        }

        .alert {
          padding: 12px 16px;
          border-radius: 8px;
          margin-bottom: 24px;
          display: flex;
          align-items: center;
          gap: 12px;
        }

        .alert-error {
          background-color: #fee;
          border: 1px solid #fcc;
          color: #c33;
        }

        .alert-icon {
          width: 20px;
          height: 20px;
          flex-shrink: 0;
        }

        .form-group {
          margin-bottom: 24px;
        }

        .form-group label {
          display: block;
          font-size: 14px;
          font-weight: 600;
          color: #2d3748;
          margin-bottom: 8px;
        }

        .form-input {
          width: 100%;
          padding: 12px 16px;
          font-size: 14px;
          border: 2px solid #e2e8f0;
          border-radius: 8px;
          transition: all 0.2s;
        }

        .form-input:focus {
          outline: none;
          border-color: #667eea;
          box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }

        .form-input:disabled {
          background-color: #f7fafc;
          cursor: not-allowed;
        }

        .form-help {
          display: block;
          font-size: 12px;
          color: #718096;
          margin-top: 6px;
        }

        .btn {
          padding: 12px 24px;
          font-size: 14px;
          font-weight: 600;
          border-radius: 8px;
          border: none;
          cursor: pointer;
          transition: all 0.2s;
          display: inline-flex;
          align-items: center;
          justify-content: center;
          gap: 8px;
        }

        .btn-primary {
          background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
          color: white;
        }

        .btn-primary:hover:not(:disabled) {
          transform: translateY(-2px);
          box-shadow: 0 8px 20px rgba(102, 126, 234, 0.4);
        }

        .btn-primary:disabled {
          opacity: 0.6;
          cursor: not-allowed;
        }

        .btn-block {
          width: 100%;
        }

        .btn-icon {
          width: 18px;
          height: 18px;
        }

        .spinner {
          width: 18px;
          height: 18px;
          animation: spin 1s linear infinite;
        }

        .spinner-circle {
          stroke-dasharray: 60;
          stroke-dashoffset: 45;
          animation: spinCircle 1.5s ease-in-out infinite;
        }

        @keyframes spin {
          to {
            transform: rotate(360deg);
          }
        }

        @keyframes spinCircle {
          0% {
            stroke-dashoffset: 60;
          }
          50% {
            stroke-dashoffset: 0;
          }
          100% {
            stroke-dashoffset: -60;
          }
        }

        .snowflake-login-footer {
          text-align: center;
          margin-top: 24px;
          padding-top: 24px;
          border-top: 1px solid #e2e8f0;
        }

        .snowflake-login-footer p {
          font-size: 14px;
          color: #718096;
        }

        .snowflake-login-footer a {
          color: #667eea;
          text-decoration: none;
          font-weight: 600;
        }

        .snowflake-login-footer a:hover {
          text-decoration: underline;
        }
      `}</style>
    </div>
  );
};

export default SnowflakeLogin;
