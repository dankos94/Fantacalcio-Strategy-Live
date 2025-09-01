package it.costantino.astaapp.service.impl;

import it.costantino.astaapp.domain.Giocatore;
import it.costantino.astaapp.domain.Squadra;
import it.costantino.astaapp.domain.Roster;
import it.costantino.astaapp.domain.enumeration.Role;
import it.costantino.astaapp.repository.GiocatoreRepository;
import it.costantino.astaapp.repository.SquadraRepository;
import it.costantino.astaapp.repository.RosterRepository;
import it.costantino.astaapp.service.GiocatoreService;
import it.costantino.astaapp.service.dto.GiocatoreDTO;
import it.costantino.astaapp.service.mapper.GiocatoreMapper;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * Service Implementation for managing {@link it.costantino.astaapp.domain.Giocatore}.
 */
@Service
@Transactional
public class GiocatoreServiceImpl implements GiocatoreService {

    private static final Logger LOG = LoggerFactory.getLogger(GiocatoreServiceImpl.class);

    private final GiocatoreRepository giocatoreRepository;

    private final GiocatoreMapper giocatoreMapper;
    
    private final SquadraRepository squadraRepository;
    
    private final RosterRepository rosterRepository;

    public GiocatoreServiceImpl(GiocatoreRepository giocatoreRepository, GiocatoreMapper giocatoreMapper, 
                               SquadraRepository squadraRepository, RosterRepository rosterRepository) {
        this.giocatoreRepository = giocatoreRepository;
        this.giocatoreMapper = giocatoreMapper;
        this.squadraRepository = squadraRepository;
        this.rosterRepository = rosterRepository;
    }

    @Override
    public GiocatoreDTO save(GiocatoreDTO giocatoreDTO) {
        LOG.debug("Request to save Giocatore : {}", giocatoreDTO);
        Giocatore giocatore = giocatoreMapper.toEntity(giocatoreDTO);
        giocatore = giocatoreRepository.save(giocatore);
        return giocatoreMapper.toDto(giocatore);
    }

    @Override
    public GiocatoreDTO update(GiocatoreDTO giocatoreDTO) {
        LOG.debug("Request to update Giocatore : {}", giocatoreDTO);
        Giocatore giocatore = giocatoreMapper.toEntity(giocatoreDTO);
        giocatore = giocatoreRepository.save(giocatore);
        return giocatoreMapper.toDto(giocatore);
    }

    @Override
    public Optional<GiocatoreDTO> partialUpdate(GiocatoreDTO giocatoreDTO) {
        LOG.debug("Request to partially update Giocatore : {}", giocatoreDTO);

        return giocatoreRepository
            .findById(giocatoreDTO.getId())
            .map(existingGiocatore -> {
                giocatoreMapper.partialUpdate(existingGiocatore, giocatoreDTO);

                return existingGiocatore;
            })
            .map(giocatoreRepository::save)
            .map(giocatoreMapper::toDto);
    }

    @Override
    @Transactional(readOnly = true)
    public List<GiocatoreDTO> findAll() {
        LOG.debug("Request to get all Giocatores");
        return giocatoreRepository.findAll().stream().map(giocatoreMapper::toDto).collect(Collectors.toCollection(LinkedList::new));
    }

    @Override
    @Transactional(readOnly = true)
    public Optional<GiocatoreDTO> findOne(Long id) {
        LOG.debug("Request to get Giocatore : {}", id);
        return giocatoreRepository.findById(id).map(giocatoreMapper::toDto);
    }

    @Override
    public void delete(Long id) {
        LOG.debug("Request to delete Giocatore : {}", id);
        giocatoreRepository.deleteById(id);
    }

    @Override
    @Transactional
    public GiocatoreDTO purchasePlayer(Long playerId, Long squadraId) {
        LOG.debug("Request to purchase Player : {} for Team : {}", playerId, squadraId);
        
        // Find the player
        Giocatore giocatore = giocatoreRepository.findById(playerId)
            .orElseThrow(() -> new IllegalArgumentException("Player not found with id: " + playerId));
        
        // Check if player is already owned by a team
        if (giocatore.getSquadra() != null) {
            throw new IllegalArgumentException("Player is already owned by team: " + giocatore.getSquadra().getNome());
        }
        
        // Find the team
        Squadra squadra = squadraRepository.findById(squadraId)
            .orElseThrow(() -> new IllegalArgumentException("Team not found with id: " + squadraId));
        
        // Assign player to team
        giocatore.setSquadra(squadra);
        giocatoreRepository.save(giocatore);
        
        // Update roster counters based on player role
        Role playerRole = giocatore.getRole();
        if (playerRole != null) {
            // Find or create roster for the team
            Optional<Roster> optionalRoster = rosterRepository.findBySquadraId(squadraId);
            Roster roster;
            
            if (optionalRoster.isPresent()) {
                roster = optionalRoster.get();
            } else {
                // Create new roster if it doesn't exist
                roster = new Roster();
                roster.setSquadra(squadra);
                roster.setFull(false);
                roster.setPort(0L);
                roster.setDif(0L);
                roster.setCc(0L);
                roster.setAtt(0L);
            }
            
            // Increment the appropriate role counter
            switch (playerRole) {
                case GK:
                    roster.setPort(roster.getPort() + 1);
                    break;
                case DF:
                    roster.setDif(roster.getDif() + 1);
                    break;
                case MF:
                    roster.setCc(roster.getCc() + 1);
                    break;
                case FW:
                    roster.setAtt(roster.getAtt() + 1);
                    break;
            }
            
            rosterRepository.save(roster);
        }
        
        return giocatoreMapper.toDto(giocatore);
    }
}
