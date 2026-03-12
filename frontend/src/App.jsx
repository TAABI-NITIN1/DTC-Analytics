import { useState, useEffect } from 'react';
import { gql, useQuery } from '@apollo/client';
import Sidebar from './components/Sidebar';
import FleetPage from './pages/FleetPage';
import DtcPage from './pages/DtcPage';
import VehiclePage from './pages/VehiclePage';
import CustomerPage from './pages/CustomerPage';
import MaintenancePage from './pages/MaintenancePage';
import ChatWidget from './components/ChatWidget';

const CUSTOMER_VEHICLES_QUERY = gql`
  query CustomerVehicles($customerName: String!) {
    customerVehicles(customerName: $customerName) {
      uniqueid
      vehicleNumber
    }
  }
`;

function App() {
  const [selectedCustomer, setSelectedCustomer] = useState(null);
  const [activeLevel, setActiveLevel] = useState('customer');
  const [days, setDays] = useState(30);
  const [dtcCode, setDtcCode] = useState('P0420');
  const [uniqueid, setUniqueid] = useState('');

  // Reset to customer page when customer deselected
  useEffect(() => {
    if (!selectedCustomer) setActiveLevel('customer');
  }, [selectedCustomer]);

  // Fetch vehicles for selected customer (for dropdown)
  const { data: vehData } = useQuery(CUSTOMER_VEHICLES_QUERY, {
    variables: { customerName: selectedCustomer || '' },
    skip: !selectedCustomer,
  });
  const customerVehicles = vehData?.customerVehicles ?? [];

  const handleSelectCustomer = (name) => {
    setSelectedCustomer(name);
    setActiveLevel('fleet');
    setUniqueid('');
  };

  const handleClearCustomer = () => {
    setSelectedCustomer(null);
    setActiveLevel('customer');
    setUniqueid('');
  };

  return (
    <div className="layout">
      <Sidebar
        activeLevel={activeLevel}
        onChange={setActiveLevel}
        selectedCustomer={selectedCustomer}
      />
      <main className="content">
        {/* Customer badge header */}
        {selectedCustomer && (
          <div className="customer-badge-bar">
            <span className="customer-badge">
              <span className="customer-badge-icon">👤</span>
              {selectedCustomer}
              <button className="customer-badge-close" onClick={handleClearCustomer} title="Clear customer">×</button>
            </span>
          </div>
        )}

        {/* Toolbar — only visible when customer selected and not on customer page */}
        {selectedCustomer && activeLevel !== 'customer' && (
          <header className="toolbar card">
            <div className="toolbar-row">
              <label>
                Time Range (days)
                <input
                  type="number"
                  min={1}
                  max={365}
                  value={days}
                  onChange={(e) => setDays(Number(e.target.value || 30))}
                />
              </label>
              <label>
                DTC Code
                <input
                  type="text"
                  value={dtcCode}
                  onChange={(e) => setDtcCode(e.target.value.toUpperCase())}
                  placeholder="P0420"
                />
              </label>
              <label>
                Vehicle
                <select value={uniqueid} onChange={(e) => setUniqueid(e.target.value)}>
                  <option value="">-- select vehicle --</option>
                  {customerVehicles.map((v) => (
                    <option key={v.uniqueid} value={v.uniqueid}>
                      {v.vehicleNumber || v.uniqueid}
                    </option>
                  ))}
                </select>
              </label>
            </div>
          </header>
        )}

        {activeLevel === 'customer' ? (
          <CustomerPage onSelectCustomer={handleSelectCustomer} />
        ) : null}
        {activeLevel === 'fleet' ? (
          <FleetPage days={days} customerName={selectedCustomer} />
        ) : null}
        {activeLevel === 'dtc' ? (
          <DtcPage days={days} dtcCode={dtcCode} customerName={selectedCustomer} />
        ) : null}
        {activeLevel === 'vehicle' ? (
          <VehiclePage days={days} uniqueid={uniqueid} customerName={selectedCustomer} />
        ) : null}
        {activeLevel === 'maintenance' ? (
          <MaintenancePage customerName={selectedCustomer} />
        ) : null}
      </main>
      <ChatWidget pageContext={{
        mode: activeLevel === 'customer' ? 'fleet' : activeLevel,
        customer_name: selectedCustomer || '',
        vehicle_number: customerVehicles.find(v => v.uniqueid === uniqueid)?.vehicleNumber || '',
        dtc_code: dtcCode || '',
      }} />
    </div>
  );
}

export default App;