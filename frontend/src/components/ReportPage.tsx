import React, { useState, useEffect } from 'react';
import { useKeycloak } from '@react-keycloak/web';

const ReportPage: React.FC = () => {
  const { keycloak, initialized } = useKeycloak();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [userInfo, setUserInfo] = useState<any>(null);
  const [rows, setRows] = useState<any[]>([]);
  const [startDate, setStartDate] = useState<string>('');
  const [endDate, setEndDate] = useState<string>('');

  // Загрузка информации о пользователе при инициализации
  useEffect(() => {
    const loadUserInfo = async () => {
      if (keycloak?.authenticated && keycloak?.token) {
        try {
          const userInfo = await keycloak.loadUserInfo();
          setUserInfo(userInfo);
        } catch (err) {
          console.error('Failed to load user info:', err);
        }
      }
    };

    if (initialized) {
      loadUserInfo();
    }
  }, [keycloak, initialized]);

  const downloadReport = async () => {
    if (!keycloak?.token) {
      setError('Not authenticated');
      return;
    }

    try {
      setLoading(true);
      setError(null);

      // Обновление токена
      const refreshed = await keycloak.updateToken(30);
      if (refreshed) {
        console.log('Token refreshed');
      }

      const params = new URLSearchParams();
      if (startDate) params.append('start_date', startDate);
      if (endDate) params.append('end_date', endDate);
      const url = `${process.env.REACT_APP_API_URL}/reports${params.toString() ? `?${params.toString()}` : ''}`;

      const response = await fetch(url, {
        headers: {
          'Authorization': `Bearer ${keycloak.token}`
        }
      });

      if (!response.ok) {
        let detail = `HTTP error! status: ${response.status}`;
        try {
          const body = await response.json();
          if (body?.detail) detail = body.detail;
        } catch {}
        throw new Error(detail);
      }
      const data = await response.json();
      setRows(Array.isArray(data) ? data : []);
      
    } catch (err) {
      setError(err instanceof Error ? err.message : 'An error occurred');
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    keycloak?.logout();
  };

  if (!initialized) {
    return <div>Loading...</div>;
  }

  if (!keycloak.authenticated) {
    return (
      <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
        <button
          onClick={() => keycloak.login()}
          className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
        >
          Login
        </button>
      </div>
    );
  }

  return (
    <div className="flex flex-col items-center justify-center min-h-screen bg-gray-100">
      <div className="p-8 bg-white rounded-lg shadow-md w-full max-w-md">
        <div className="flex justify-between items-center mb-6">
          <h1 className="text-2xl font-bold">Usage Reports</h1>
          <button
            onClick={handleLogout}
            className="px-3 py-1 text-sm bg-gray-500 text-white rounded hover:bg-gray-600"
          >
            Logout
          </button>
        </div>

        {userInfo && (
          <div className="mb-6 p-4 bg-blue-50 rounded-lg">
            <h2 className="text-lg font-semibold mb-2">Welcome!</h2>
            <p className="text-sm text-gray-600">
              <strong>Name:</strong> {userInfo.name || userInfo.preferred_username}
            </p>
            <p className="text-sm text-gray-600">
              <strong>Email:</strong> {userInfo.email}
            </p>
            {userInfo.realm_access?.roles && (
              <p className="text-sm text-gray-600">
                <strong>Roles:</strong> {userInfo.realm_access.roles.join(', ')}
              </p>
            )}
          </div>
        )}
        
        <div className="grid grid-cols-1 gap-3 mb-4">
          <label className="text-sm text-gray-700">Start date</label>
          <input
            type="date"
            value={startDate}
            onChange={(e) => setStartDate(e.target.value)}
            className="border rounded px-2 py-1"
          />
          <label className="text-sm text-gray-700">End date</label>
          <input
            type="date"
            value={endDate}
            onChange={(e) => setEndDate(e.target.value)}
            className="border rounded px-2 py-1"
          />
        </div>

        <button
          onClick={downloadReport}
          disabled={loading}
          className={`w-full px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600 ${
            loading ? 'opacity-50 cursor-not-allowed' : ''
          }`}
        >
          {loading ? 'Loading Report...' : 'Get Report'}
        </button>

        {error && (
          <div className="mt-4 p-4 bg-red-100 text-red-700 rounded">
            {error}
          </div>
        )}

        {rows.length > 0 && (
          <div className="mt-6 overflow-x-auto">
            <table className="min-w-full text-sm">
              <thead>
                <tr className="text-left text-gray-600">
                  <th className="px-2 py-1">Date</th>
                  <th className="px-2 py-1">Steps</th>
                  <th className="px-2 py-1">Avg Battery</th>
                  <th className="px-2 py-1">Avg Load (kg)</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r, idx) => (
                  <tr key={idx} className="border-t">
                    <td className="px-2 py-1">{r.usage_date}</td>
                    <td className="px-2 py-1">{r.steps_sum ?? '-'}</td>
                    <td className="px-2 py-1">{r.avg_battery ?? '-'}</td>
                    <td className="px-2 py-1">{r.load_avg ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
};

export default ReportPage;