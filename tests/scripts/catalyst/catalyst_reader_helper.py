'''
The target dataset: catalyst particle
Root folder: /gdata/RAVEN/lamni/2020-1/comm_33IDD
'''
import numpy as np
import h5py as h5

class CatalystReader():

    def __init__(self, root_path='./micdata2/lamni/2020-1/comm_33IDD'):
        self.root_path = root_path

    def read_catalyst_data(self, scanId,
            clip_center_yx=(260, 771),
            clip_size = 64, #x+64, x-64
            # below are constants for this dataset
            detector_distance = np.float32(2), # (m)
            detector_pixel_size = np.float32(7.5e-5), #(m)
            incident_wavelength = np.float32(1.4089112), # (angstrom)
            lamino_angle   = np.float32(61.18),
            tilt_angle     = np.float32(-72.035),
            skewness_angle = np.float32(-2.724),
            fft_shift=True):

        # Compute diffraction data
        original_measurement_file = f"{self.root_path}/raw_data/scan{scanId}_data_000001.h5" 
        original_measurement_path = 'entry/data/data'
        original_measurement_h5 = h5.File(original_measurement_file, 'r')
        idata = np.array(original_measurement_h5[original_measurement_path])
        if clip_center_yx is not None:
            idata = idata[:, 
                            clip_center_yx[0]-clip_size:clip_center_yx[0]+clip_size,
                            clip_center_yx[1]-clip_size:clip_center_yx[1]+clip_size]
        if fft_shift: idata = np.fft.ifftshift(idata, axes=(-2, -1))

        # Read original, real positions and probe data.
        probe_file = f"{self.root_path}/analysis/S00000-00999/S00{scanId}/affineS00{scanId}_128x128_b0_MLc_L1_p7_g50_bg0.001_mm_recons.h5"
        pf = h5.File(probe_file, 'r')
        probe_initial = np.array(pf['reconstruction/p/probe_initial']).reshape((7,128,128)).astype(np.complex64)
        probes = np.array(pf['reconstruction/p/probes/probe_0']).astype(np.complex64)
        positions_orig = np.array(pf['reconstruction/p/positions_orig']).astype(np.float32)
        #positions_orig[:, [0,1]] = positions_orig[:, [1,0]] * -1 # swap axis (x,y) and * -1
        positions_orig[:, [0,1]] = positions_orig[:, [0,1]] * np.array([-1, -1])
        positions_real = np.array(pf['reconstruction/p/positions_real']).astype(np.float32)
        #positions_real[:, [0,1]] = positions_real[:, [1,0]] # swap axis (x,y)
        positions_real[:, [0,1]] = positions_real[:, [0,1]] * np.array([1, 1])

        # Get the rotation angles for all scans
        f_rotation_angles = f"{self.root_path}/specES1/dat-files/tomography_scannumbers.txt"
        rots = {}
        with open(f_rotation_angles) as f_rot:
            for line in f_rot.readlines():
                arr = line.split()
                rots[np.int32(arr[0])] = np.float32(arr[2])

        ddict = {
            'scanId': scanId,
            'detector_distance': detector_distance,
            'detector_pixel_size': detector_pixel_size,
            'incident_wavelength': incident_wavelength,
            'rotation_angle': rots[scanId],
            'lamino_angle': lamino_angle,
            'tilt_angle': tilt_angle,
            'skewness_angle': skewness_angle,
            'data': idata,
            'positions_orig': positions_orig, # original position information
            'positions_real': positions_real, # fixed position information
            'recprobe': probes,
            'initprobe': probe_initial
        }

        return ddict
    
    
    def normalize_catalyst_data(self, ddict, use_fixed_positions=False, use_fixed_probes=False, pos2pix_const=34068864.0):
        scan = ddict['positions_real'][:]*pos2pix_const if use_fixed_positions else ddict['positions_orig'][:]*pos2pix_const
        scan = np.array(scan, dtype='float32', order='C')
        
        data = ddict['data']
        if len(ddict['data']) != len(scan):
            print(
                f"The number of positions {scan.shape} and frames {ddict['data'].shape}"
                " is not equal. One of the two will be truncated.")
            num_frame = min(len(ddict['data']), len(scan))
            scan = scan[:num_frame, ...]
            data = data[:num_frame, ...]
        
        probe = ddict['recprobe'] if use_fixed_probes else ddict['initprobe']
        probe = probe[np.newaxis, np.newaxis, ...]

        return scan, data, probe
    

    def read_catalyst_data_range(self, mrange=(241, 361)): # [j,i)
        all_data = []
        for i in range(mrange[0], mrange[1]):
            try:
                print("Extracting scanId={}".format(i))
                d = self.read_catalyst_data(i)
                print("Done extracting scanId={}".format(i))
                all_data.append(d)
            except Exception as e: #(FileNotFoundError, IOError) as e:
                print("error caught in scanId={}: {}".format(i, e))
                continue;
        return all_data
        
