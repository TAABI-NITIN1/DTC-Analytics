import { useEffect, useMemo, useRef, useState } from 'react';
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
  const [dtcCode, setDtcCode] = useState('');
  const [uniqueid, setUniqueid] = useState('');
  const [vehicleSearchText, setVehicleSearchText] = useState('');
  const [debouncedVehicleSearchText, setDebouncedVehicleSearchText] = useState('');
  const [showVehicleSuggestions, setShowVehicleSuggestions] = useState(false);
  const [activeVehicleSuggestionIndex, setActiveVehicleSuggestionIndex] = useState(-1);
  const vehicleSearchRef = useRef(null);

  // Reset to customer page when customer deselected
  useEffect(() => {
    if (!selectedCustomer) setActiveLevel('customer');
  }, [selectedCustomer]);

  // Fetch vehicles for selected customer (for dropdown)
  const { data: vehData, loading: vehiclesLoading } = useQuery(CUSTOMER_VEHICLES_QUERY, {
    variables: { customerName: selectedCustomer || '' },
    skip: !selectedCustomer,
  });
  const customerVehicles = vehData?.customerVehicles ?? [];

  useEffect(() => {
    const timer = setTimeout(() => {
      setDebouncedVehicleSearchText(vehicleSearchText);
    }, 250);
    return () => clearTimeout(timer);
  }, [vehicleSearchText]);

  useEffect(() => {
    if (!selectedCustomer) {
      setVehicleSearchText('');
      setDebouncedVehicleSearchText('');
      setShowVehicleSuggestions(false);
      setActiveVehicleSuggestionIndex(-1);
    }
  }, [selectedCustomer]);

  useEffect(() => {
    if (!uniqueid) {
      return;
    }
    const selected = customerVehicles.find((v) => v.uniqueid === uniqueid);
    if (selected) {
      setVehicleSearchText(selected.vehicleNumber || selected.uniqueid);
    }
  }, [uniqueid, customerVehicles]);

  useEffect(() => {
    const handleClickOutside = (event) => {
      if (vehicleSearchRef.current && !vehicleSearchRef.current.contains(event.target)) {
        setShowVehicleSuggestions(false);
        setActiveVehicleSuggestionIndex(-1);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const filteredVehicles = useMemo(() => {
    const query = debouncedVehicleSearchText.trim().toLowerCase();
    if (query.length < 3) {
      return [];
    }
    return customerVehicles
      .filter((vehicle) => String(vehicle.vehicleNumber || vehicle.uniqueid).toLowerCase().includes(query))
      .slice(0, 20);
  }, [debouncedVehicleSearchText, customerVehicles]);

  const selectVehicleAndNavigate = (vehicle) => {
    if (!vehicle?.uniqueid) {
      return;
    }
    setUniqueid(vehicle.uniqueid);
    setVehicleSearchText(vehicle.vehicleNumber || vehicle.uniqueid);
    setShowVehicleSuggestions(false);
    setActiveVehicleSuggestionIndex(-1);
    setActiveLevel('vehicle');
  };

  const handleVehicleSearchKeyDown = (event) => {
    if (!showVehicleSuggestions || filteredVehicles.length === 0) {
      return;
    }
    if (event.key === 'ArrowDown') {
      event.preventDefault();
      setActiveVehicleSuggestionIndex((prev) => (prev + 1) % filteredVehicles.length);
      return;
    }
    if (event.key === 'ArrowUp') {
      event.preventDefault();
      setActiveVehicleSuggestionIndex((prev) => (prev <= 0 ? filteredVehicles.length - 1 : prev - 1));
      return;
    }
    if (event.key === 'Enter') {
      if (activeVehicleSuggestionIndex >= 0) {
        event.preventDefault();
        selectVehicleAndNavigate(filteredVehicles[activeVehicleSuggestionIndex]);
      }
      return;
    }
    if (event.key === 'Escape') {
      setShowVehicleSuggestions(false);
      setActiveVehicleSuggestionIndex(-1);
    }
  };

  const handleSelectCustomer = (name) => {
    setSelectedCustomer(name);
    setActiveLevel('fleet');
    setUniqueid('');
    setVehicleSearchText('');
    setDtcCode('');
  };

  const handleClearCustomer = () => {
    setSelectedCustomer(null);
    setActiveLevel('customer');
    setUniqueid('');
    setVehicleSearchText('');
    setDtcCode('');
  };

  const handleVehicleSelect = (selectedUniqueid) => {
    if (!selectedUniqueid) {
      return;
    }
    const vehicle = customerVehicles.find((v) => v.uniqueid === selectedUniqueid) || { uniqueid: selectedUniqueid, vehicleNumber: selectedUniqueid };
    selectVehicleAndNavigate(vehicle);
  };

  const handleDtcSelect = (selectedDtcCode) => {
    const nextCode = String(selectedDtcCode || '').trim().toUpperCase();
    if (!nextCode) {
      return;
    }
    setDtcCode(nextCode);
    setActiveLevel('dtc');
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
                  placeholder="Enter DTC code"
                />
              </label>
              <label ref={vehicleSearchRef} style={{ position: 'relative' }}>
                Vehicle
                <input
                  type="text"
                  value={vehicleSearchText}
                  onChange={(e) => {
                    setVehicleSearchText(e.target.value);
                    setUniqueid('');
                    setShowVehicleSuggestions(true);
                    setActiveVehicleSuggestionIndex(-1);
                  }}
                  onFocus={() => setShowVehicleSuggestions(true)}
                  onKeyDown={handleVehicleSearchKeyDown}
                  placeholder="Type 3+ chars to search vehicle"
                  autoComplete="off"
                />
                {showVehicleSuggestions && (
                  <div className="typeahead-menu">
                    {vehicleSearchText.trim().length < 3 ? (
                      <div className="typeahead-empty">Type at least 3 characters</div>
                    ) : vehiclesLoading ? (
                      <div className="typeahead-empty">Loading vehicles...</div>
                    ) : filteredVehicles.length === 0 ? (
                      <div className="typeahead-empty">No matching vehicles found</div>
                    ) : (
                      filteredVehicles.map((v, index) => (
                        <button
                          key={v.uniqueid}
                          type="button"
                          className={`typeahead-option ${index === activeVehicleSuggestionIndex ? 'active' : ''}`}
                          onClick={() => selectVehicleAndNavigate(v)}
                        >
                          <span className="typeahead-primary">{v.vehicleNumber || v.uniqueid}</span>
                          {v.vehicleNumber && <span className="typeahead-secondary">{v.uniqueid}</span>}
                        </button>
                      ))
                    )}
                  </div>
                )}
              </label>
            </div>
          </header>
        )}

        {activeLevel === 'customer' ? (
          <CustomerPage onSelectCustomer={handleSelectCustomer} />
        ) : null}
        {activeLevel === 'fleet' ? (
          <FleetPage
            days={days}
            customerName={selectedCustomer}
            onSelectVehicle={handleVehicleSelect}
            onSelectDtc={handleDtcSelect}
          />
        ) : null}
        {activeLevel === 'dtc' ? (
          <DtcPage
            days={days}
            dtcCode={dtcCode}
            customerName={selectedCustomer}
            onSelectVehicle={handleVehicleSelect}
            onSelectDtc={handleDtcSelect}
          />
        ) : null}
        {activeLevel === 'vehicle' ? (
          <VehiclePage
            days={days}
            uniqueid={uniqueid}
            customerName={selectedCustomer}
            onSelectDtc={handleDtcSelect}
          />
        ) : null}
        {activeLevel === 'maintenance' ? (
          <MaintenancePage
            customerName={selectedCustomer}
            onSelectVehicle={handleVehicleSelect}
            onSelectDtc={handleDtcSelect}
          />
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